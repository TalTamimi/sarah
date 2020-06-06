package com.example.sarah

import kotlinx.coroutines.*
import org.springframework.stereotype.Service
import java.io.File
import java.time.Instant.*
import java.util.*
import kotlin.collections.LinkedHashMap

@Service
class DB {
    companion object {
        var memtable: TreeMap<String, String> = TreeMap()
        val Segments = LinkedHashMap<Long, TreeMap<String, String>>()
    }

    fun get(key: String): String? {
        return memtable[key] ?: Segments
                .values
                .reversed()
                .find { it[key] != null }?.get(key)
    }


    fun put(key: String, value: String) {
        memtable[key] = value

        if (memtable.size >= 50) //approximation
        {
            flush()
        }
    }


    private fun flush() {
        val timeStamp = now().toEpochMilli()
        Segments[timeStamp] = memtable
        memtable = TreeMap()
        val entries = Segments.values.last().entries

        GlobalScope.launch {
            writeSegmentToDisk(timeStamp, entries)
        }

    }

    private fun writeSegmentToDisk(timestamp: Long, entries: MutableSet<MutableMap.MutableEntry<String, String>>) {
        File("./data/${timestamp}.sst")
                .bufferedWriter()
                .use { out -> entries.forEach { out.write("${it.key}\t${it.value}\n") } }
    }


}


