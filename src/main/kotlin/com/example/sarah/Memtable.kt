package com.example.sarah

import kotlinx.coroutines.*
import java.io.File
import java.time.Instant.*
import java.util.*
import kotlin.collections.Map.*

class Memtable() {
    companion object {
        var memtable: TreeMap<String, String> = TreeMap()
    }

    fun get(key: String) = memtable[key]


    fun put(key: String, value: String) {
        memtable[key] = value

        if (memtable.size >= 50) //approximation
        {
           flush()
        }
    }


    private fun flush() {
        val oldMemtable = memtable
        memtable = TreeMap()
        val entries = oldMemtable.entries

        GlobalScope.launch {
            flush()
        }

        writeSegmentToDisk(entries)
    }

    private fun writeSegmentToDisk(entries: Set<Entry<String, String>>) {
        File("./data/${now().toEpochMilli()}.sst")
                .bufferedWriter()
                .use { out -> entries.forEach { out.write("${it.key}\t${it.value}\n") } }
    }


}

