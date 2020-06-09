package com.example.sarah

import kotlinx.coroutines.*
import org.springframework.stereotype.Service
import java.io.File
import java.time.Instant.*
import java.util.*
import kotlin.collections.LinkedHashMap

@Service
class DB {
    var memtable: TreeMap<String, String> = TreeMap()
    val segments = LinkedHashMap<String, TreeMap<String, String>>()

    fun clear(){
        memtable.clear()
        segments.clear()
    }

    fun get(key: String): String? {
        return memtable[key] ?: segments
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

        synchronized(segments) {
            val uniqueFileIdentifier = "${now().toEpochMilli()}_${segments.size}"
            segments[uniqueFileIdentifier] = memtable
            memtable = TreeMap()
            val entries = segments.values.last().entries

            GlobalScope.launch {
                writeSegmentToDisk(uniqueFileIdentifier, entries)
            }
        }


    }

    private fun writeSegmentToDisk(fileName: String, entries: MutableSet<MutableMap.MutableEntry<String, String>>) {
        File("./data/${fileName}.sst")
                .bufferedWriter()
                .use { out -> entries.forEach { out.write("${it.key}\t${it.value}\n") } }
    }


}


