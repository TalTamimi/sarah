package com.example.sarah.service

import com.example.sarah.domain.BufferedRows
import java.io.File


class MergeProcessor(private val db: DB) {

    var lastKey = "___"

    fun bufferedCompaction(filesSequence: MutableList<File>, newFile: File) {
        val buffers = filesSequence.map { BufferedRows(it, it.name) }.toMutableList()
        newFile.createNewFile()
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
                            buffers.removeAt(0)
                    }
                }
        db.refreshSegments(newFile, filesSequence)
    }
}

