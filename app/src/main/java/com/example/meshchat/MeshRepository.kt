package com.example.meshchat

import android.content.Context
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

class MeshRepository(context: Context) {
    private val dao = AppDatabase.getInstance(context).messageDao()

    suspend fun insertMessage(entity: MessageEntity) {
        dao.insert(entity)
    }

    fun getAllMessages(): Flow<List<MeshMessage>> {
        return dao.getAllMessagesFlow().map { list -> list.map { MessageEntity.toMeshMessage(it) } }
    }

    fun getMessagesForHash(hash: String): Flow<List<MeshMessage>> {
        return dao.getMessagesForHashFlow(hash).map { list -> list.map { MessageEntity.toMeshMessage(it) } }
    }
}
