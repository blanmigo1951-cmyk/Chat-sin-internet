package com.example.meshchat

import android.Manifest
import android.app.*
import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothManager
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.location.LocationManager
import android.os.Build
import android.os.Bundle
import android.os.IBinder
import android.provider.Settings
import android.util.Log
import android.widget.Toast
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.activity.viewModels
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.core.app.NotificationCompat
import androidx.core.content.ContextCompat
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewModelScope
import com.google.android.gms.nearby.Nearby
import com.google.android.gms.nearby.connection.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import org.json.JSONObject
import java.security.MessageDigest
import java.util.LinkedHashMap
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

// MessageBus unchanged
object MessageBus {
    private val _incoming = MutableSharedFlow<MeshMessage>(extraBufferCapacity = 100)
    val incoming = _incoming.asSharedFlow()

    fun emit(msg: MeshMessage) {
        _incoming.tryEmit(msg)
    }
}

object ServiceStateBus {
    private val _running = MutableStateFlow(false)
    val running: StateFlow<Boolean> = _running.asStateFlow()

    fun setRunning(r: Boolean) {
        _running.value = r
    }
}

// MeshMessage unchanged
data class MeshMessage(
    val id: String = UUID.randomUUID().toString(),
    val senderHash: String,
    val targetHash: String,
    val senderName: String,
    val text: String,
    var hops: Int = 5
) {
    fun toJson(): String = JSONObject().apply {
        put("id", id)
        put("senderHash", senderHash)
        put("targetHash", targetHash)
        put("senderName", senderName)
        put("text", text)
        put("hops", hops)
    }.toString()

    companion object {
        fun fromJson(jsonStr: String): MeshMessage? {
            return try {
                val json = JSONObject(jsonStr)
                MeshMessage(
                    id = json.optString("id", UUID.randomUUID().toString()),
                    senderHash = json.optString("senderHash", ""),
                    targetHash = json.optString("targetHash", "BROADCAST"),
                    senderName = json.optString("senderName", "Desconocido"),
                    text = json.optString("text", ""),
                    hops = json.optInt("hops", 5)
                )
            } catch (e: Exception) {
                null
            }
        }

        fun hashPhoneNumber(phone: String): String {
            val cleanPhone = phone.replace(Regex("[^0-9]"), "")
            val bytes = MessageDigest.getInstance("SHA-256").digest(cleanPhone.toByteArray())
            return bytes.joinToString("") { "%02x".format(it) }
        }
    }
}

// MeshService: KEEP as before but remove direct DB logic (ViewModel/Repository handles persistence via MessageBus)
class MeshService(private val context: Context, myPhoneNumber: String) {
    private val TAG = "MeshService"
    private val connectionsClient = Nearby.getConnectionsClient(context)
    private val myHash = MeshMessage.hashPhoneNumber(myPhoneNumber)

    private val activeConnections = ConcurrentHashMap.newKeySet<String>()
    private val pendingConnections = ConcurrentHashMap.newKeySet<String>()

    // LRU thread-safe
    private val processedMessageIdsLock = Any()
    private val processedMessageIds = object : LinkedHashMap<String, Unit>(1024, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, Unit>): Boolean {
            return size > 1000
        }
    }

    private fun isAlreadyProcessed(id: String): Boolean = synchronized(processedMessageIdsLock) {
        processedMessageIds.containsKey(id)
    }

    private fun markProcessed(id: String) = synchronized(processedMessageIdsLock) {
        processedMessageIds[id] = Unit
    }

    private val SERVICE_ID = "com.example.meshchat.SERVICE"

    private fun hasPermissions(): Boolean {
        val fineLocation = ContextCompat.checkSelfPermission(context, Manifest.permission.ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            val btScan = ContextCompat.checkSelfPermission(context, Manifest.permission.BLUETOOTH_SCAN) == PackageManager.PERMISSION_GRANTED
            val btAdv = ContextCompat.checkSelfPermission(context, Manifest.permission.BLUETOOTH_ADVERTISE) == PackageManager.PERMISSION_GRANTED
            val btConnect = ContextCompat.checkSelfPermission(context, Manifest.permission.BLUETOOTH_CONNECT) == PackageManager.PERMISSION_GRANTED
            return fineLocation && btScan && btAdv && btConnect
        }
        return fineLocation
    }

    fun startMeshNetwork() {
        if (!hasPermissions()) {
            Log.w(TAG, "Permisos insuficientes para iniciar Nearby Connections.")
            return
        }

        try {
            connectionsClient.startAdvertising(
                myHash,
                SERVICE_ID,
                connectionLifecycleCallback,
                AdvertisingOptions.Builder().setStrategy(Strategy.P2P_CLUSTER).build()
            )
            connectionsClient.startDiscovery(
                SERVICE_ID,
                endpointDiscoveryCallback,
                DiscoveryOptions.Builder().setStrategy(Strategy.P2P_CLUSTER).build()
            )
            Log.d(TAG, "Nearby advertising + discovery iniciados")
        } catch (e: Exception) {
            Log.e(TAG, "Error al iniciar Nearby Connections", e)
        }
    }

    fun stopMeshNetwork() {
        try {
            connectionsClient.stopAdvertising()
            connectionsClient.stopDiscovery()
            connectionsClient.stopAllEndpoints()
            activeConnections.clear()
            pendingConnections.clear()
            Log.d(TAG, "Nearby detenido y estados limpiados")
        } catch (e: Exception) {
            Log.w(TAG, "Error al detener Nearby Connections", e)
        }
    }

    private val endpointDiscoveryCallback = object : EndpointDiscoveryCallback() {
        override fun onEndpointFound(endpointId: String, info: DiscoveredEndpointInfo) {
            if (!activeConnections.contains(endpointId) && !pendingConnections.contains(endpointId)) {
                pendingConnections.add(endpointId)
                connectionsClient.requestConnection(myHash, endpointId, connectionLifecycleCallback)
                    .addOnFailureListener { e ->
                        pendingConnections.remove(endpointId)
                        Log.w(TAG, "Error al solicitar conexión a $endpointId", e)
                    }
            }
        }
        override fun onEndpointLost(endpointId: String) {
            pendingConnections.remove(endpointId)
        }
    }

    private val connectionLifecycleCallback = object : ConnectionLifecycleCallback() {
        override fun onConnectionInitiated(endpointId: String, info: ConnectionInfo) {
            connectionsClient.acceptConnection(endpointId, payloadCallback)
        }

        override fun onConnectionResult(endpointId: String, result: ConnectionResolution) {
            pendingConnections.remove(endpointId)
            if (result.status.isSuccess) {
                activeConnections.add(endpointId)
                Log.d(TAG, "Conectado con $endpointId")
            } else {
                Log.w(TAG, "Conexión rechazada o fallida con $endpointId: ${result.status.statusMessage}")
            }
        }

        override fun onDisconnected(endpointId: String) {
            activeConnections.remove(endpointId)
            pendingConnections.remove(endpointId)
            Log.d(TAG, "Desconectado $endpointId")
        }
    }

    private val payloadCallback = object : PayloadCallback() {
        override fun onPayloadReceived(endpointId: String, payload: Payload) {
            payload.asBytes()?.let {
                val msg = MeshMessage.fromJson(String(it, Charsets.UTF_8))
                if (msg != null) {
                    processIncomingMessage(msg, endpointId)
                }
            }
        }
        override fun onPayloadTransferUpdate(endpointId: String, update: PayloadTransferUpdate) {}
    }

    private fun processIncomingMessage(msg: MeshMessage, incomingEndpointId: String?) {
        if (isAlreadyProcessed(msg.id)) return
        markProcessed(msg.id)

        if (msg.targetHash == myHash || msg.targetHash == "BROADCAST") {
            // Emite para que ViewModel/Repository persista y UI actualice
            MessageBus.emit(msg)
        }

        if (msg.hops > 1 && msg.targetHash != myHash) {
            msg.hops -= 1
            broadcastToNeighbors(msg, incomingEndpointId)
        }
    }

    fun sendMessage(targetPhoneNumber: String, senderName: String, text: String): MeshMessage {
        val targetHash = if (targetPhoneNumber == "BROADCAST") "BROADCAST" else MeshMessage.hashPhoneNumber(targetPhoneNumber)
        val msg = MeshMessage(senderHash = myHash, targetHash = targetHash, senderName = senderName, text = text)
        markProcessed(msg.id)
        broadcastToNeighbors(msg, null)
        return msg
    }

    private fun broadcastToNeighbors(msg: MeshMessage, excludeEndpointId: String?) {
        val payload = Payload.fromBytes(msg.toJson().toByteArray(Charsets.UTF_8))
        activeConnections.filter { it != excludeEndpointId }.forEach { endpointId ->
            connectionsClient.sendPayload(endpointId, payload)
                .addOnSuccessListener {
                    Log.d(TAG, "Payload enviado correctamente a $endpointId")
                }
                .addOnFailureListener { e ->
                    Log.w(TAG, "Error al enviar payload a $endpointId", e)
                }
        }
    }
}

// Foreground service reads SharedPreferences if no extra provided and uses notification icon from drawable
class MeshForegroundService : Service() {
    companion object {
        const val EXTRA_PHONE_NUMBER = "extra_phone_number"
        const val PREFS_NAME = "mesh_prefs"
        const val PREF_PHONE_KEY = "my_phone_number"
        var instance: MeshForegroundService? = null
            private set
    }

    var meshService: MeshService? = null
        private set

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val prefs = applicationContext.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
        val phoneNumber = intent?.getStringExtra(EXTRA_PHONE_NUMBER)
            ?: prefs.getString(PREF_PHONE_KEY, "+573000000000")!!

        createNotificationChannel()
        val notification = NotificationCompat.Builder(this, "MESH_CHANNEL")
            .setContentTitle("Red Mesh SOS Activa")
            .setContentText("Buscando y reenviando mensajes de emergencia...")
            .setSmallIcon(R.drawable.ic_notification) // vector drawable added
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .build()

        startForeground(1, notification)

        meshService?.stopMeshNetwork()
        meshService = MeshService(applicationContext, phoneNumber)
        meshService?.startMeshNetwork()

        instance = this
        ServiceStateBus.setRunning(true)

        return START_STICKY
    }

    override fun onDestroy() {
        meshService?.stopMeshNetwork()
        meshService = null
        instance = null
        ServiceStateBus.setRunning(false)
        super.onDestroy()
    }

    override fun onBind(intent: Intent?): IBinder? = null

    private fun createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val channel = NotificationChannel("MESH_CHANNEL", "Mesh SOS Service", NotificationManager.IMPORTANCE_LOW)
            getSystemService(NotificationManager::class.java)?.createNotificationChannel(channel)
        }
    }
}

// ViewModel uses repository for persistence and exposes flows from DB
class MeshViewModel(application: Application) : AndroidViewModel(application) {
    companion object {
        private const val PREFS_NAME = "mesh_prefs"
        private const val PREF_PHONE_KEY = "my_phone_number"
    }

    private val repo = MeshRepository(application.applicationContext)

    private val _meshState = MutableStateFlow(MeshState.WAITING_PERMISSIONS)
    val meshState: StateFlow<MeshState> = _meshState.asStateFlow()

    // Expose messages as Flow from repository
    val messages: Flow<List<MeshMessage>> = repo.getAllMessages()

    private val prefs = application.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
    private val initialPhone = prefs.getString(PREF_PHONE_KEY, "+573000000000") ?: "+573000000000"
    private val _phoneNumber = MutableStateFlow(initialPhone)
    val phoneNumber: StateFlow<String> = _phoneNumber.asStateFlow()

    init {
        viewModelScope.launch {
            // Collect MessageBus and persist incoming messages
            MessageBus.incoming.collect { msg ->
                repo.insertMessage(MessageEntity.fromMeshMessage(msg))
            }
        }
        viewModelScope.launch {
            ServiceStateBus.running.collect { isRunning ->
                _meshState.update { currentState ->
                    if (isRunning) MeshState.RUNNING
                    else if (currentState != MeshState.WAITING_PERMISSIONS) MeshState.READY
                    else currentState
                }
            }
        }
    }

    fun setPermissionsGranted() {
        if (_meshState.value == MeshState.WAITING_PERMISSIONS) {
            _meshState.value = if (ServiceStateBus.running.value) MeshState.RUNNING else MeshState.READY
        }
    }

    fun sendMessage(targetPhoneNumber: String, senderName: String, text: String) {
        val activeMesh = MeshForegroundService.instance?.meshService
        if (activeMesh != null) {
            val sentMsg = activeMesh.sendMessage(targetPhoneNumber, senderName, text)
            // Persist sent message
            viewModelScope.launch {
                repo.insertMessage(MessageEntity.fromMeshMessage(sentMsg))
            }
        } else {
            Toast.makeText(getApplication(), "El servicio de malla no está activo", Toast.LENGTH_SHORT).show()
        }
    }

    fun setPhoneNumber(newPhone: String) {
        prefs.edit().putString(PREF_PHONE_KEY, newPhone).apply()
        _phoneNumber.value = newPhone
    }
}

// UI and MainActivity logic similar to previous, but wiring to ViewModel messages Flow (from Room)
class MainActivity : ComponentActivity() {
    private val viewModel: MeshViewModel by viewModels()

    private val bluetoothEnableLauncher = registerForActivityResult(ActivityResultContracts.StartActivityForResult()) { _ ->
        if (isBluetoothEnabled()) {
            checkLocationAndStart()
        } else {
            Toast.makeText(this, "Bluetooth requerido para Nearby Connections.", Toast.LENGTH_LONG).show()
        }
    }

    private val locationSettingsLauncher = registerForActivityResult(ActivityResultContracts.StartActivityForResult()) { _ ->
        if (isLocationEnabled()) {
            checkLocationAndStart()
        } else {
            Toast.makeText(this, "La ubicación debe estar activada para Nearby Connections.", Toast.LENGTH_LONG).show()
        }
    }

    private val notificationPermissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestPermission()
    ) { isGranted ->
        if (isGranted) {
            checkLocationAndStart()
        } else {
            Toast.makeText(this, "Las notificaciones son necesarias para ejecutar el servicio en segundo plano.", Toast.LENGTH_LONG).show()
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        val permissionLauncher = registerForActivityResult(ActivityResultContracts.RequestMultiplePermissions()) { permissions ->
            if (permissions.values.all { it }) {
                viewModel.setPermissionsGranted()
                checkLocationAndStart()
            } else {
                Toast.makeText(this, "Permisos necesarios para la red Mesh.", Toast.LENGTH_LONG).show()
            }
        }

        val requiredPermissions = mutableListOf(
            Manifest.permission.ACCESS_FINE_LOCATION
        ).apply {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
                add(Manifest.permission.BLUETOOTH_SCAN)
                add(Manifest.permission.BLUETOOTH_ADVERTISE)
                add(Manifest.permission.BLUETOOTH_CONNECT)
            }
        }.toTypedArray()

        permissionLauncher.launch(requiredPermissions)

        setContent {
            MaterialTheme {
                MainScreen(
                    viewModel = viewModel,
                    onStartService = { ensureNotificationPermissionAndStart() },
                    onStopService = { stopMeshService() }
                )
            }
        }
    }

    private fun ensureNotificationPermissionAndStart() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS) == PackageManager.PERMISSION_GRANTED) {
                checkLocationAndStart()
            } else {
                notificationPermissionLauncher.launch(Manifest.permission.POST_NOTIFICATIONS)
            }
        } else {
            checkLocationAndStart()
        }
    }

    private fun checkLocationAndStart() {
        val fineLocationGranted = ContextCompat.checkSelfPermission(this, Manifest.permission.ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED
        val bluetoothScanGranted = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            ContextCompat.checkSelfPermission(this, Manifest.permission.BLUETOOTH_SCAN) == PackageManager.PERMISSION_GRANTED
        } else true

        if (!fineLocationGranted || !bluetoothScanGranted) {
            Toast.makeText(this, "Permisos de ubicación / Bluetooth no concedidos.", Toast.LENGTH_LONG).show()
            return
        }

        if (!isBluetoothEnabled()) {
            promptEnableBluetooth()
            return
        }

        if (!isLocationEnabled()) {
            promptEnableLocation()
            return
        }

        executeStartService()
    }

    private fun isBluetoothEnabled(): Boolean {
        val bluetoothManager = getSystemService(Context.BLUETOOTH_SERVICE) as? BluetoothManager
        val adapter = bluetoothManager?.adapter ?: BluetoothAdapter.getDefaultAdapter()
        return adapter?.isEnabled == true
    }

    private fun promptEnableBluetooth() {
        val enableBtIntent = Intent(BluetoothAdapter.ACTION_REQUEST_ENABLE)
        bluetoothEnableLauncher.launch(enableBtIntent)
    }

    private fun isLocationEnabled(): Boolean {
        val lm = getSystemService(Context.LOCATION_SERVICE) as? LocationManager ?: return false
        return try {
            lm.isProviderEnabled(LocationManager.GPS_PROVIDER) || lm.isProviderEnabled(LocationManager.NETWORK_PROVIDER)
        } catch (e: Exception) {
            false
        }
    }

    private fun promptEnableLocation() {
        val intent = Intent(Settings.ACTION_LOCATION_SOURCE_SETTINGS)
        locationSettingsLauncher.launch(intent)
    }

    private fun executeStartService() {
        val phone = viewModel.phoneNumber.value
        val intent = Intent(this, MeshForegroundService::class.java).apply {
            putExtra(MeshForegroundService.EXTRA_PHONE_NUMBER, phone)
        }
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            startForegroundService(intent)
        } else {
            startService(intent)
        }
    }

    private fun stopMeshService() {
        val intent = Intent(this, MeshForegroundService::class.java)
        stopService(intent)
        ServiceStateBus.setRunning(false)
    }
}

@Composable
fun MainScreen(
    viewModel: MeshViewModel,
    onStartService: () -> Unit,
    onStopService: () -> Unit
) {
    val meshState by viewModel.meshState.collectAsStateWithLifecycle()
    var searchQuery by remember { mutableStateOf("") }
    var selectedContact by remember { mutableStateOf<Contact?>(null) }
    var textInput by remember { mutableStateOf("") }

    // phone state from ViewModel
    val phoneState by viewModel.phoneNumber.collectAsStateWithLifecycle()
    var editPhone by remember { mutableStateOf(phoneState) }

    LaunchedEffect(phoneState) { editPhone = phoneState }

    val messagesFlow = viewModel.messages
    val messages by messagesFlow.collectAsStateWithLifecycle(initialValue = emptyList())

    val contactsList = remember {
        listOf(
            Contact("Mamá", "+573001234567"),
            Contact("Carlos Pérez", "+573109876543"),
            Contact("EMERGENCIA GENERAL", "BROADCAST")
        )
    }

    Column(modifier = Modifier.fillMaxSize().padding(16.dp)) {
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text("Mesh SOS", fontSize = 20.sp, color = MaterialTheme.colorScheme.primary)

            when (meshState) {
                MeshState.WAITING_PERMISSIONS -> Text("Esperando permisos...", color = Color.Gray, fontSize = 12.sp)
                MeshState.READY -> Button(onClick = onStartService) { Text("Iniciar Malla") }
                MeshState.RUNNING -> Button(onClick = onStopService, colors = ButtonDefaults.buttonColors(containerColor = Color.Red)) { Text("Detener Malla") }
            }
        }

        Spacer(modifier = Modifier.height(12.dp))

        // Campo para editar/guardar el número del usuario
        Card(modifier = Modifier.fillMaxWidth().padding(bottom = 12.dp)) {
            Row(modifier = Modifier.padding(12.dp), verticalAlignment = Alignment.CenterVertically) {
                OutlinedTextField(
                    value = editPhone,
                    onValueChange = { editPhone = it },
                    label = { Text("Mi número") },
                    modifier = Modifier.weight(1f)
                )
                Spacer(modifier = Modifier.width(8.dp))
                val ctx = LocalContext.current
                Button(onClick = {
                    val newPhone = editPhone.trim()
                    if (newPhone.isNotEmpty()) {
                        // Persist via ViewModel
                        viewModel.setPhoneNumber(newPhone)
                        // Restart service to use new number
                        onStartService()
                        Toast.makeText(ctx, "Número guardado y servicio reiniciado", Toast.LENGTH_SHORT).show()
                    } else {
                        Toast.makeText(ctx, "Introduce un número válido", Toast.LENGTH_SHORT).show()
                    }
                }) {
                    Text("Guardar")
                }
            }
        }

        if (selectedContact == null) {
            OutlinedTextField(
                value = searchQuery,
                onValueChange = { searchQuery = it },
                label = { Text("Buscar contacto por nombre...") },
                modifier = Modifier.fillMaxWidth()
            )
            Spacer(modifier = Modifier.height(8.dp))
            LazyColumn {
                items(contactsList.filter { it.name.contains(searchQuery, true) }) { contact ->
                    Card(modifier = Modifier.fillMaxWidth().padding(4.dp).clickable { selectedContact = contact }) {
                        Column(modifier = Modifier.padding(16.dp)) {
                            Text(contact.name, fontSize = 18.sp)
                            Text(contact.phoneNumber, fontSize = 14.sp, color = Color.Gray)
                        }
                    }
                }
            }
        } else {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Button(onClick = { selectedContact = null }) { Text("← Volver") }
                Spacer(modifier = Modifier.width(8.dp))
                Text("Chat: ${selectedContact!!.name}", fontSize = 18.sp)
            }

            val filteredMessages by remember(messages, selectedContact, phoneState) {
                derivedStateOf {
                    val contact = selectedContact ?: return@derivedStateOf emptyList()
                    if (contact.phoneNumber == "BROADCAST") {
                        messages.filter { it.targetHash == "BROADCAST" }
                    } else {
                        val contactHash = MeshMessage.hashPhoneNumber(contact.phoneNumber)
                        val myHash = MeshMessage.hashPhoneNumber(phoneState)
                        messages.filter { msg ->
                            msg.senderHash == contactHash ||
                            msg.targetHash == contactHash ||
                            (msg.senderHash == myHash && msg.targetHash == contactHash)
                        }
                    }
                }
            }

            LazyColumn(modifier = Modifier.weight(1f).fillMaxWidth()) {
                items(filteredMessages) { msg ->
                    Surface(color = MaterialTheme.colorScheme.primaryContainer, shape = RoundedCornerShape(8.dp), modifier = Modifier.padding(4.dp)) {
                        Column(modifier = Modifier.padding(8.dp)) {
                            Text(msg.senderName, fontSize = 12.sp, color = Color.Gray)
                            Text(msg.text, fontSize = 16.sp)
                        }
                    }
                }
            }
            Row(modifier = Modifier.fillMaxWidth()) {
                OutlinedTextField(value = textInput, onValueChange = { textInput = it }, modifier = Modifier.weight(1f))
                Spacer(modifier = Modifier.width(8.dp))
                Button(onClick = {
                    if (textInput.isNotBlank() && selectedContact != null) {
                        viewModel.sendMessage(selectedContact!!.phoneNumber, "Yo", textInput)
                        textInput = ""
                    }
                }) { Text("Enviar") }
            }
        }
    }
}
