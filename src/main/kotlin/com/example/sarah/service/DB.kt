package com.example.sarah.service

import com.example.sarah.domain.Row
import com.example.sarah.filewriter.SSTFile
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.springframework.stereotype.Service
import java.io.File
import java.time.Instant.*
import java.util.*
import kotlin.collections.LinkedHashMap
import kotlin.collections.MutableMap.*

@Service
final class DB {
    var memtable: TreeMap<String, String> = TreeMap()
    private val segments = LinkedHashMap<SSTFile, TreeMap<String, String>?>()
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
                       file.scanByKey(key)
                    else
                        memTable[key]
                }
                .firstOrNull()
    }

    fun put(key: String, value: String) {
        memtable[key] = value

        if (memtable.size >= 10) //approximation
        {
            flush()
        }
    }

    fun refreshSegments(addedFile: File, RemovedFiles: MutableList<SSTFile>) {
        segments[SSTFile(addedFile, true)] = null
        RemovedFiles
                .stream()
                .forEach { segments.remove(it) }

    }

    private fun flush() {
        synchronized(segments) {
            val file = SSTFile(generateNewSSTFileName(), false)
            segments[file] = memtable
            val tmp = memtable
            memtable = TreeMap()
            val entries = tmp.entries
            GlobalScope.launch {
                file.writeToDisk(entries)
            }

            if (segments.size > 50 && mergeProcessor.reserveForProcessing())
                GlobalScope.launch {
                    mergeProcessor.bufferedCompaction(segments.keys.toMutableList(), generateNewSSTFileName())
                }
        }
    }

    private fun generateNewSSTFileName(): File {
        val uniqueFileIdentifier = "${now().toEpochMilli()}_${segments.size}"
        return File("./data/${uniqueFileIdentifier}.sst")
    }


}


