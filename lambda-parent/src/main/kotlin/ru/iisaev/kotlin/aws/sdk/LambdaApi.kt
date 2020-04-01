package ru.iisaev.kotlin.aws.sdk

import com.amazonaws.services.lambda.runtime.*
import kotlinx.coroutines.runBlocking
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

abstract class LambdaHandler<I, O>(protected val context: Context) {

    fun log(message: String) = context.logger.log(message)

    abstract suspend fun handle(input: I): O
}

abstract class LambdaRequestHandlerFactory<I, O>
    : RequestHandler<I, O> {

    abstract fun getHandler(context: Context): LambdaHandler<I, O>

    @ExperimentalTime
    override fun handleRequest(input: I, context: Context): O? {
        return if (input is Map<*, *> && input["actionType"] == "PING") {
            context.logger.log("Pong\n")
            null
        } else {
            measureTimedValue {
                runBlocking {
                    getHandler(context).handle(input)
                }
            }.let { (result, requestTime) ->
                context.logger.log("Finished function execution in $requestTime")
                result
            }
        }
    }
}

class LocalInvocationContext : Context {
    override fun getAwsRequestId(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getLogStreamName(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getClientContext(): ClientContext {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getFunctionName(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getRemainingTimeInMillis(): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getLogger() = object : LambdaLogger {
        override fun log(message: String?) {
            println(message)
        }

        override fun log(message: ByteArray?) {
            println(message)
        }
    }

    override fun getInvokedFunctionArn(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getMemoryLimitInMB(): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getLogGroupName(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getFunctionVersion(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getIdentity(): CognitoIdentity {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}