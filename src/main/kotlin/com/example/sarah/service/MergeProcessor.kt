package com.example.sarah.service

import com.example.sarah.domain.BufferedRows
import com.example.sarah.filewriter.SSTFile
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import java.io.File
import java.util.concurrent.atomic.AtomicBoolean


class MergeProcessor(private val db: DB) {

    private val isProcessingFiles = AtomicBoolean(false)

    var lastKey = "___"


    fun reserveForProcessing() =
            isProcessingFiles.compareAndSet(false, true)


    suspend fun bufferedCompaction(filesSequence: MutableList<SSTFile>, newFile: File) {
        waitForAllFilesToBeWritten(filesSequence)
        val buffers = filesSequence.map { BufferedRows(it.getFile(), it.getName()) }.toMutableList()
        withContext(Dispatchers.IO) {
            newFile.createNewFile()
        }
        newFile.writer()
                .buffered()
                .use {
                    while (buffers.isNotEmpty()) {
                        buffers.sort()
                        val smallestKeyRow = buffers[0].get()
                        if (smallestKeyRow.key != lastKey) {
                            it.write(smallestKeyRow.toString())
                            lastKey = smallestKeyRow.key
                        }
                        if (!buffers[0].next())
                            buffers[0].close()
                        buffers.removeAt(0)
                    }
                }

        db.refreshSegments(newFile, filesSequence)
        isProcessingFiles.set(false)
    }


    private suspend fun waitForAllFilesToBeWritten(filesSequence: MutableList<SSTFile>) {
        filesSequence
                .forEach {
                    while (!it.isFlushed)
                        delay(20)
                }
    }
}

