package com.example.sarah.filewriter

import com.example.sarah.domain.Row
import java.io.File

class SSTFile(private val file: File, var isFlushed: Boolean) {
    fun writeToDisk(entries: MutableSet<MutableMap.MutableEntry<String, String>>) {
        file.bufferedWriter()
                .use { out -> entries.forEach { out.write("${it.key}\t${it.value}\n") } }
        isFlushed = true
    }

    fun getName(): String = file.name
    fun getFile(): File = file //todo: close this endpoint and not expose internal file
    fun scanByKey(key: String): String? =
            file
                    .readLines()
                    .map { Row.RowContructor(it, "") }//todo change Row type to specialized type
                    .filter { it.key == key }
                    .map { it.value }
                    .firstOrNull()


}