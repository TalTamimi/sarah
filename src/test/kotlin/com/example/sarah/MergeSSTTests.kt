package com.example.sarah

import com.example.sarah.domain.Row
import com.example.sarah.service.MergeProcessor
import kotlinx.coroutines.*
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.setMain
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.util.ResourceUtils
import java.io.File
import java.time.Instant
import java.util.*

@SpringBootTest(classes = [MergeProcessor::class])
@RunWith(SpringRunner::class)
class MergeSSTTests {

    @Autowired
    private lateinit var mergeProcessor: MergeProcessor
    private val mainThreadSurrogate = newSingleThreadContext("singleThreadForCoroutinesTest")


    @Before
    fun setUp() {
        Dispatchers.setMain(mainThreadSurrogate)
    }

    @After
    fun tearDown() {
        Dispatchers.resetMain()
        mainThreadSurrogate.close()
    }


//    @Test
//    fun firstSSTMergingAlgorithmTest() {
//        runBlocking {
//            launch(Dispatchers.Main) {
//                val ftm = System.currentTimeMillis()
//                val files = ResourceUtils.getFile("classpath:simpleSample")
//                        .walkTopDown()
//                        .filter { it.isFile }
//
//                mergeProcessor.inMemoryMergeSort(files)
//                val expected = ResourceUtils.getFile("classpath:testAssertion/expectedSSTMerge.sst").readLines().toString()
//                val actual = ResourceUtils.getFile("./data/distenctSstMerge.sst").readLines().toString()
//                val ltm = System.currentTimeMillis()
//
//                println("First merging algorithm took: ${ltm - ftm}")
//                Assert.assertEquals(expected, actual)
//            }
//        }
//    }

//    @Test
//    fun writeToDisk() {
//        GlobalScope.launch {
//            File("./data/1591468789967.sst")
//                    .bufferedWriter()
//                    .use {
//                        for (i in 100..250) {
//                            it.write("${i}\t${UUID.randomUUID()}\n")
//                        }
//                    }
//        }
//    }

    @Test
    fun secondSSTMergingAlgorithmTest() {
        runBlocking {
            launch(Dispatchers.Main) {
                val ftm = System.currentTimeMillis()

                val files = ResourceUtils.getFile("classpath:simpleSample")
                        .walkTopDown()
                        .filter { it.isFile }

                mergeProcessor.bufferedMergeSort(files)
                val expected = ResourceUtils.getFile("classpath:testAssertion/expectedSSTMerge2.sst").readLines().toString()
                val actual = ResourceUtils.getFile("./data/distenctSstMerge2.sst").readLines().toString()
                val ltm = System.currentTimeMillis()

                println("Second merging algorithm took: ${ltm - ftm}")

                Assert.assertEquals(expected, actual)
            }
        }
    }

}