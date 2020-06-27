package com.example.sarah.domain

import com.example.sarah.domain.Row.Companion.RowContructor
import java.io.BufferedReader
import java.io.File
import java.lang.RuntimeException

class BufferedRows(file: File, private val index: String) : AutoCloseable, Comparable<BufferedRows> {

    private val bufferedReader: BufferedReader = file.bufferedReader()
    private var current: Row

    init {
        val nextBuffer = bufferedReader.readLine() ?: throw RuntimeException("Compacting Empty File")
        current = RowContructor(nextBuffer, index)
        next()
    }


    fun get() = current

    fun next(): Boolean {
        val nextBuffer = bufferedReader.readLine()
        return if (nextBuffer == null)
            false
        else {
            current = RowContructor(nextBuffer, index)
            true
        }
    }

    override fun close() {
        bufferedReader.close()
    }

    override fun compareTo(other: BufferedRows): Int {
        return when {
            current.key < other.current.key -> -1
            current.key > other.current.key -> 1
            else -> current.index.compareTo(other.index)
        }
    }

}
