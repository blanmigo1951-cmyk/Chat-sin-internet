package com.example.meshchat

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import kotlinx.coroutines.flow.Flow

@Dao
interface MessageDao {
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun insert(message: MessageEntity)

    @Query("SELECT * FROM messages ORDER BY timestamp ASC")
    fun getAllMessagesFlow(): Flow<List<MessageEntity>>

    @Query("SELECT * FROM messages WHERE senderHash = :hash OR targetHash = :hash OR targetHash = 'BROADCAST' ORDER BY timestamp ASC")
    fun getMessagesForHashFlow(hash: String): Flow<List<MessageEntity>>
}
