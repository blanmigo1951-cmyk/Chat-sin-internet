package com.example.meshchat

import androidx.room.Entity
import androidx.room.PrimaryKey

@Entity(tableName = "messages")
data class MessageEntity(
    @PrimaryKey val id: String,
    val senderHash: String,
    val targetHash: String,
    val senderName: String,
    val text: String,
    val hops: Int,
    val timestamp: Long
) {
    companion object {
        fun fromMeshMessage(m: MeshMessage): MessageEntity {
            return MessageEntity(
                id = m.id,
                senderHash = m.senderHash,
                targetHash = m.targetHash,
                senderName = m.senderName,
                text = m.text,
                hops = m.hops,
                timestamp = System.currentTimeMillis()
            )
        }

        fun toMeshMessage(e: MessageEntity): MeshMessage {
            return MeshMessage(
                id = e.id,
                senderHash = e.senderHash,
                targetHash = e.targetHash,
                senderName = e.senderName,
                text = e.text,
                hops = e.hops
            )
        }
    }
}
