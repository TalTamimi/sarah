package com.example.sarah

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class SarahApplication

fun main(args: Array<String>) {
    runApplication<SarahApplication>(*args)
}
