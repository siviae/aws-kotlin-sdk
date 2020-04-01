package ru.iisaev.kotlin.aws.sdk

import kotlinx.coroutines.*

suspend fun <A, B> Iterable<A>.pmap(concurrency: Int = 10, f: suspend (A) -> B): List<B> = coroutineScope {
    chunked(concurrency).flatMap { chunk -> chunk.map { async { f(it) } }.awaitAll() }
}

suspend fun <A, B> Collection<A>.pmap(concurrency: Int = this.size, f: suspend (A) -> B): List<B> = coroutineScope {
    if (size == 0) {
        emptyList()
    } else {
        chunked(concurrency).flatMap { chunk -> chunk.map { async { f(it) } }.awaitAll() }
    }
}

suspend fun <A> Iterable<A>.pforEach(concurrency: Int = 10, f: suspend (A) -> Unit): Unit = coroutineScope {
    chunked(concurrency).forEach { chunk -> chunk.map { launch { f(it) } }.joinAll() }
}

suspend fun <A> Collection<A>.pforEach(concurrency: Int = this.size, f: suspend (A) -> Unit): Unit = coroutineScope {
    if (size != 0) {
        chunked(concurrency).forEach { chunk -> chunk.map { launch { f(it) } }.joinAll() }
    }
}