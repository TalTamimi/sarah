package com.example.sarah.domain

import java.io.File
import kotlin.streams.toList

data class BufferedRows(val rows: List<Row>, val fileIndex: Int, val offset: Long, val lineIndex: Int) {

    fun getNextCandidate(f: MutableList<File>): BufferedRows? {
        return if ((this.lineIndex + 1) < this.rows.size) {
            this.copy(lineIndex = this.lineIndex + 1)
        } else {
            val newDataBuffer = BufferedRows(f[this.fileIndex]
                    .bufferedReader()
                    .lines()
                    .skip(this.offset)
                    .limit(100)
                    .map {
                        val split = it.split("\t")
                        Row(split[0], split[1])
                    }
                    .toList(), this.fileIndex, this.offset + 100, 0)

            if (newDataBuffer.rows.isNotEmpty()) {
                return newDataBuffer
            }
            return null
        }

    }
}
