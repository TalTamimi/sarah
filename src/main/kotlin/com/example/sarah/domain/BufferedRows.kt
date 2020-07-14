package com.example.sarah.domain

import com.example.sarah.domain.Row.Companion.RowContructor
import java.io.BufferedReader
import java.io.File
import java.lang.RuntimeException

class BufferedRows(file: File, private val index: String) : AutoCloseable, Comparable<BufferedRows> {

    private val bufferedReader: BufferedReader = file.bufferedReader()
    private var currentRow: Row

    init {
        val firstLine = bufferedReader
                .readLine()
                ?: throw RuntimeException("Compacting Empty File")
        currentRow = RowContructor(firstLine, index)
        next()
    }


    fun get() = currentRow

    fun next(): Boolean {
        val nextBuffer = bufferedReader.readLine()
        return if (nextBuffer == null)
            false
        else {
            currentRow = RowContructor(nextBuffer, index)
            true
        }
    }

    override fun close() {
        bufferedReader.close()
    }

    override fun compareTo(other: BufferedRows): Int {
        return when {
            currentRow.key < other.currentRow.key -> -1
            currentRow.key > other.currentRow.key -> 1
            else -> currentRow.index.compareTo(other.index)
        }
    }

}
