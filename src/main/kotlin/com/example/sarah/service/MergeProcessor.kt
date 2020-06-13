package com.example.sarah.service

import com.example.sarah.domain.BufferedRows
import com.example.sarah.domain.CompactionStrategy.BUFFERED_COMPACTION_STRATEGY
import com.example.sarah.domain.CompactionStrategy.IN_MEMORY_COMPACTION_STRATEGY
import com.example.sarah.domain.RowNode
import com.example.sarah.domain.Row
import com.example.sarah.util.SSTableCompactor
import org.springframework.stereotype.Service
import java.io.File
import java.util.*
import kotlin.streams.toList

@Suppress("UNCHECKED_CAST")
@Service
class MergeProcessor {

    fun inMemoryCompaction(files: Sequence<File>) {
        val mergedSST = SSTableCompactor(IN_MEMORY_COMPACTION_STRATEGY)

        files.forEach { file ->
                    file.useLines { lines ->
                        lines.map { row ->
                            val rowSplit = row.split("\t")
                            Row(rowSplit[0], rowSplit[1])
                        }.forEach {
                            mergedSST.replaceOrInsert(it)
                        }
                    }
                }

        mergedSST.sort().flush("distenctSstMerge")
    }


    fun bufferedCompaction(filesSequence: Sequence<File>) {
        val mergedSST = SSTableCompactor(BUFFERED_COMPACTION_STRATEGY)
        val cq = PriorityQueue<RowNode>()
        val files = filesSequence.toMutableList()

        val listOfBufferedFiles = files.mapIndexed { index, file ->
            BufferedRows(file.bufferedReader().lines().limit(100).map {
                val split = it.split("\t")
                Row(split[0], split[1])
            }.toList(), index, 100, 0)
        }.toMutableList()

        listOfBufferedFiles.forEach {
            cq.add(RowNode.fromBufferedRows(it))
        }

        while (cq.isNotEmpty()) {
            val winner = cq.poll()
            mergedSST.replaceOrInsert(winner.row)
            val newCandidate = listOfBufferedFiles[winner.fileIndex].getNextCandidate(files)
            if (newCandidate != null) {
                listOfBufferedFiles[winner.fileIndex] = newCandidate
                cq.add(RowNode.fromBufferedRows(newCandidate))
            }
        }

        mergedSST.flush("distenctSstMerge2")
    }

//    private fun replaceOrInsert(mergedSST: MutableList<Row>, row: Row, mergeAlgorithmType: MergeAlgorithm) {
//        val index = mergeAlgorithmType.search(mergedSST, row)
//        if (index >= 0) {
//            mergedSST[index] = row
//            return
//        }
//        mergedSST.add(row)
//    }


}
