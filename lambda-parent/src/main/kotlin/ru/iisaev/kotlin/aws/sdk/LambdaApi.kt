package ru.iisaev.kotlin.aws.sdk

import com.amazonaws.services.lambda.runtime.*
import kotlinx.coroutines.*
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

interface AwsClient {
    val httpClient: SdkAsyncHttpClient
}

object AwsClientSingleton : AwsClient {
    override val httpClient: SdkAsyncHttpClient by lazy {
        NettyNioAsyncHttpClient
                .builder()
                .build()
    }
}

abstract class LambdaHandler<I, O>(protected val context: Context,
                                   private val client: AwsClient) : AwsClient by client {

    fun log(message: String) = context.logger.log(message)

    abstract suspend fun handle(input: I): O
}

abstract class LambdaRequestHandlerFactory<I, O>
    : RequestHandler<I, O>, AwsClient by AwsClientSingleton {

    abstract fun getHandler(context: Context, awsClient: AwsClient): LambdaHandler<I, O>

    @ExperimentalTime
    override fun handleRequest(input: I, context: Context): O? {
        return if (input is Map<*, *> && input["actionType"] == "PING") {
            context.logger.log("Pong\n")
            null
        } else {
            measureTimedValue {
                runBlocking {
                    getHandler(context, AwsClientSingleton).handle(input)
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