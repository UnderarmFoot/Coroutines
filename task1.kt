/**
 * Пропускает только первое значение в рамках заданного временного окна.
 * Все последующие значения, пришедшие пока окно активно, игнорируются.
 */
fun <T> Flow<T>.throttleFirst(windowMillis: Long): Flow<T> = flow {
    require(windowMillis > 0)

    var lastEmitTime = 0L

    collect { value ->
        val currentTime = System.currentTimeMillis()

        if (currentTime - lastEmitTime >= windowMillis) {
            lastEmitTime = currentTime
            emit(value)
        }
    }
}

/**
 * Пропускает первое значение сразу, а затем отправляет последнее значение,
 * полученное в рамках каждого временного окна.
 */
fun <T> Flow<T>.throttleLatest(windowMillis: Long): Flow<T> = channelFlow {
    require(windowMillis > 0)

    var latestItem: T? = null
    var hasLatest = false
    var timerJob: Job? = null

    suspend fun emitLatestAndRestartTimer() {
        if (hasLatest) {
            val item = latestItem as T

            latestItem = null
            hasLatest = false

            send(item)

            timerJob = launch {
                delay(windowMillis)
                emitLatestAndRestartTimer()
            }
        } else {
            // За текущее окно новых значений не пришло (остановка окна)
            timerJob = null
        }
    }

    collect { item ->
        if (timerJob == null) {
            // Первое значение нового окна отправляется сразу.
            send(item)

            timerJob = launch {
                delay(windowMillis)
                emitLatestAndRestartTimer()
            }
        } else {
            // В активном окне хранится только последнее значение.
            latestItem = item
            hasLatest = true
        }
    }
}
