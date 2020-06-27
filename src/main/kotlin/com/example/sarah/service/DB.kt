package com.example.sarah.service

import com.example.sarah.domain.Row
import kotlinx.coroutines.*
import org.springframework.stereotype.Service
import java.io.File
import java.time.Instant.*
import java.util.*
import kotlin.collections.LinkedHashMap
import kotlin.collections.MutableMap.*

@Service
class DB {
    var memtable: TreeMap<String, String> = TreeMap()
    val segments = LinkedHashMap<File, TreeMap<String, String>?>()
    private val mergeProcessor = MergeProcessor(this)


    fun clear() {
        memtable.clear()
        segments.clear()
    }

    fun get(key: String): String? {
        return memtable[key] ?: segments
                .entries
                .reversed()
                .mapNotNull { (file, memTable) ->
                    if (memTable == null)
                        findFromFile(file, key)
                    else
                        memTable[key]
                }
                .firstOrNull()
    }

    fun findFromFile(file: File, key: String): String? {
        print(file.name)
        return file
                .readLines()
                .map { Row.RowContructor(it, "") }//todo change Row type to specialized type
                .filter { it.key == key }
                .map { it.value }
                .firstOrNull()
    }


    fun put(key: String, value: String) {
        memtable[key] = value

        if (memtable.size >= 10) //approximation
        {
            flush()
        }
    }


    fun flush() {
        synchronized(segments) {
            val file = generateNewSSTFileName()
            segments[file] = memtable
            val tmp = memtable
            memtable = TreeMap()
            val entries = tmp.entries


            GlobalScope.launch {
                writeSegmentToDisk(file, entries)
            }
        }
    }

    fun generateNewSSTFileName(): File {
        val uniqueFileIdentifier = "${now().toEpochMilli()}_${segments.size}"
        return File("./data/${uniqueFileIdentifier}.sst")
    }

    suspend fun writeSegmentToDisk(file: File, entries: MutableSet<MutableEntry<String, String>>) {

        file.bufferedWriter()
                .use { out -> entries.forEach { out.write("${it.key}\t${it.value}\n") } }
        if (segments.size > 50) //todo fix Launch Prematurely
            GlobalScope.launch {
                mergeProcessor.bufferedCompaction(segments.keys.toMutableList(), generateNewSSTFileName())
            }
    }

    fun refreshSegments(addedFile: File, RemovedFiles: MutableList<File>) {
        segments[addedFile] = null
        RemovedFiles
                .stream()
                .forEach { segments.remove(it) }

    }


}


