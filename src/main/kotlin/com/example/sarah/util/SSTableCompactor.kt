package com.example.sarah.util

import com.example.sarah.domain.CompactionStrategy
import com.example.sarah.domain.Row
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import java.io.File

class SSTableCompactor(private val compactionStrategy: CompactionStrategy) {

    private var compactionList: MutableList<Row> = mutableListOf()

    fun replaceOrInsert(row: Row): SSTableCompactor {
        val index = compactionStrategy.search(this.compactionList, row)

        if (index >= 0) {
            this.compactionList[index] = row
            return this
        }
        this.compactionList.add(row)
        return this
    }

    fun sort(): SSTableCompactor {
        this.compactionList = this.compactionList.sortedWith(compareBy(compactionStrategy) { it }).toMutableList()
        return this
    }

    fun flush(filename: String) {
        GlobalScope.launch {
            File("./data/$filename.sst")
                    .bufferedWriter()
                    .use { compactionList.forEach { row -> it.write(row.toString()) } }
        }
    }
}
