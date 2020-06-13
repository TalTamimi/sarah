package com.example.sarah.service

import com.example.sarah.domain.Data
import com.example.sarah.domain.QueueNode
import com.example.sarah.domain.Row
import com.example.sarah.util.AlphabetComparator
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.springframework.stereotype.Service
import java.io.File
import java.util.*
import java.util.Collections.binarySearch
import kotlin.streams.toList

@Suppress("UNCHECKED_CAST")
@Service
class MergeProcessor {

    fun inMemoryMergeSort(files: Sequence<File>) {
        val mergedSST = mutableListOf<Row>()

        files
                .forEach { file ->
                    file.useLines { lines ->
                        lines.forEach {
                            val rowSplit = it.split("\t")
                            replaceOrInsert(mergedSST, Row(rowSplit[0], rowSplit[1]))
                        }
                    }
                }
        writeToDisk(mergedSST.sortedWith(compareBy(AlphabetComparator()) { it.key }), "distenctSstMerge")
    }


    fun bufferedMergeSort(filesSequence: Sequence<File>) {
        val mergedSST2 = mutableListOf<Row>()
        val cq = PriorityQueue<QueueNode>()

        val files = filesSequence.toMutableList()

        val listOfBufferedFiles = files.mapIndexed { index, file ->
            Data(file.bufferedReader().lines().limit(100).map {
                val split = it.split("\t")
                Row(split[0], split[1])
            }.toList(), index, 100, 0)
        }.toMutableList()

        listOfBufferedFiles.forEach {
            cq.add(QueueNode.fromData(it))
        }

        while (cq.isNotEmpty()) {
            val winner = cq.poll()
            replaceOrInsert(mergedSST2, winner.row)
            val newCandidate = listOfBufferedFiles[winner.fileIndex].getNextCandidate(files)
            if (newCandidate != null) {
                listOfBufferedFiles[winner.fileIndex] = newCandidate
                cq.add(QueueNode.fromData(newCandidate))
            }
        }


        writeToDisk(mergedSST2, "distenctSstMerge2")
    }

    private fun replaceOrInsert(mergedSST: MutableList<Row>, row: Row) {
        val binarySearchBy = binarySearch(mergedSST, row)
        if (binarySearchBy >= 0) {
            mergedSST[binarySearchBy] = row
            return
        }
        mergedSST.add(row)
    }

    private fun writeToDisk(mergedSST: List<Row>, filename: String) {
        GlobalScope.launch {
            File("./data/$filename.sst")
                    .bufferedWriter()
                    .use { mergedSST.forEach { row -> it.write(row.toString()) } }
        }
    }


}
