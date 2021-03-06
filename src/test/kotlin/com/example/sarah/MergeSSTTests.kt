package com.example.sarah

import com.example.sarah.filewriter.SSTFile
import com.example.sarah.service.DB
import com.example.sarah.service.MergeProcessor
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.setMain
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.springframework.util.ResourceUtils
import org.springframework.util.ResourceUtils.getFile
import java.io.File


class MergeSSTTests {


    private val db = DB()
    private val mergeProcessor = MergeProcessor(db)
    private val mainThreadSurrogate = newSingleThreadContext("singleThreadForCoroutinesTest")

    @Before
    fun setUp() {
        Dispatchers.setMain(mainThreadSurrogate)
        recreateFolder( File("./data/"))
    }

    @After
    fun tearDown() {
        Dispatchers.resetMain()
        mainThreadSurrogate.close()
    }


    @Test
    fun firstSSTMergingAlgorithmTest() {
        runBlocking {
            launch(Dispatchers.Main) {
                val ftm = System.currentTimeMillis()
                val files = getFile("classpath:simpleSample")
                        .walkTopDown()
                        .filter { it.isFile }
                        .map { SSTFile(it, true) }

                val postCompaction = File("./data/compact.sst")
                mergeProcessor.bufferedCompaction(files.toMutableList(), postCompaction)
                val expected = getFile("classpath:testAssertion/expectedSSTMerge.sst").readLines().toString()
                val actual = postCompaction.readLines().toString()
                val ltm = System.currentTimeMillis()


                println("merging algorithm took: ${ltm - ftm}")
                Assert.assertEquals(expected, actual)
            }
        }
    }

}