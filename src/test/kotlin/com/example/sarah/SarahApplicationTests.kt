package com.example.sarah

import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.event.annotation.BeforeTestClass
import java.io.File
import java.util.UUID.randomUUID

@SpringBootTest
class SarahApplicationTests {

    @Test
    fun resetData(){
       val dataFolder= File("./data/")
        dataFolder.deleteRecursively()
        dataFolder.mkdir()
    }

    @Test
    fun contextLoads() {


        for(i in 0..3000)
            Memtable().put(randomUUID().toString(), randomUUID().toString());
    }



}
