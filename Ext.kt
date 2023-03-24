package com.alibaba.rocketmq.diagnosis.util

import com.alibaba.rocketmq.diagnosis.common.CustomGraphqlContext
import com.alibaba.rocketmq.diagnosis.common.client.MQClientHolder
import com.alibaba.rocketmq.diagnosis.common.constants.MessageConstExt
import com.alibaba.rocketmq.diagnosis.model.MapEntry
import com.alibaba.rocketmq.diagnosis.model.Message
import com.alibaba.rocketmq.diagnosis.model.SubTrace
import com.alibaba.rocketmq.diagnosis.model.TopicType
import graphql.schema.DataFetchingEnvironment
import org.apache.rocketmq.client.exception.MQBrokerException
import org.apache.rocketmq.client.impl.MQClientAPIImpl
import org.apache.rocketmq.common.message.MessageConst
import org.apache.rocketmq.common.message.MessageExt
import org.apache.rocketmq.common.metrics.rpc.RpcLabels
import org.apache.rocketmq.common.protocol.RequestCode
import org.apache.rocketmq.common.protocol.ResponseCode
import org.apache.rocketmq.common.protocol.body.GroupList
import org.apache.rocketmq.common.protocol.header.QueryTopicConsumeByWhoRequestHeader
import org.apache.rocketmq.remoting.common.RemotingHelper
import org.apache.rocketmq.remoting.protocol.RemotingCommand
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl
import java.net.InetSocketAddress

fun DataFetchingEnvironment.getCustomContext(): CustomGraphqlContext {
    return getContext()
}

fun List<MapEntry>.filter(key: List<String>): List<MapEntry> {
    if (key.isNotEmpty()) {
        val newList = mutableListOf<MapEntry>()
        val keySet = key.toHashSet()
        forEach {
            if (keySet.contains(it.key)) {
                newList.add(it)
            }
        }
        return newList
    }
    return this
}

fun MessageExt.getStoreHostString(): String? {
    if (this.storeHost != null) {
        val inetSocketAddress = this.storeHost as InetSocketAddress
        return inetSocketAddress.address.hostAddress
    }
    return null
}

fun MessageExt.type(): TopicType {
    return properties?.let {
        if (it.containsKey(MessageConstExt.DELIVER_TIME) || it.containsKey(MessageConst.PROPERTY_DELAY_TIME_LEVEL)
            || it.containsKey(MessageConst.PROPERTY_TIMER_DELIVER_MS) || it.containsKey(MessageConst.PROPERTY_TIMER_DELAY_SEC)
        ) {
            TopicType.DELAY
        } else if (it.containsKey(MessageConstExt.SHARDING_KEY) || it.containsKey(MessageConst.PROPERTY_SHARDING_KEY)) {
            TopicType.ORDER
        } else if (it.containsKey(MessageConst.PROPERTY_TRANSACTION_PREPARED)) {
            TopicType.Transaction
        } else TopicType.NORMAL
    } ?: TopicType.NORMAL
}

fun MessageExt.toMessage(): Message {
    val type = type()
    val startDeliverTime =
        if (type == TopicType.DELAY && getProperty(MessageConstExt.DELIVER_TIME) != null) {
            Utils.timeStampMilliToTime(getProperty(MessageConstExt.DELIVER_TIME).toLong())
        } else null
    return Message(
        msgId,
        getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) ?: msgId,
        getProperty(MessageConst.PROPERTY_TAGS),
        properties.map { MapEntry(it.key, it.value) },
        sysFlag,
        storeSize,
        String(body),
        Utils.timeStampMilliToTime(bornTimestamp),
        bornHostString,
        Utils.timeStampMilliToTime(storeTimestamp),
        getStoreHostString()!!,
        startDeliverTime,
        Utils.dealWithSpecialRegionId(getProperty(MessageConst.PROPERTY_MSG_REGION)),
        brokerName,
        getProperty(MessageConst.PROPERTY_FQN_TOPIC),
        queueId,
        queueOffset,
        getProperty(MessageConst.PROPERTY_MIN_OFFSET)?.toLong(),
        getProperty(MessageConst.PROPERTY_MAX_OFFSET)?.toLong(),
        type,
        getProperty(MessageConst.PROPERTY_TRACE_SWITCH)!!.toBoolean(),
        version.name,
        reconsumeTimes
    )
}

fun DefaultMQAdminExt.queryTopicConsumeByWho(topic: String, isHistoricalAccepted: Boolean): GroupList? {
    return defaultMQAdminExtImpl.queryTopicConsumeByWho(topic, isHistoricalAccepted)
}

fun DefaultMQAdminExtImpl.queryTopicConsumeByWho(topic: String, isHistoricalAccepted: Boolean): GroupList? {
    val topicRouteData = this.examineTopicRouteInfo(topic)

    for (bd in topicRouteData.brokerDatas) {
        val addr = bd.selectBrokerAddr()
        if (addr != null) {
            return mqClientInstance.mqClientAPIImpl.queryTopicConsumeByWho(addr,
                topic,
                isHistoricalAccepted,
                MQClientHolder.TIMEOUT)
        }
    }
    return null
}

fun MQClientAPIImpl.queryTopicConsumeByWho(
    addr: String,
    topic: String,
    isHistoricalAccepted: Boolean,
    timeoutMillis: Long,
): GroupList {
    val requestHeader = QueryTopicConsumeByWhoRequestHeader()
    requestHeader.topic = topic
    requestHeader.isHistoricalAccepted = isHistoricalAccepted

    val request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPIC_CONSUME_BY_WHO, requestHeader)
    RemotingHelper.updateInvocationContextForMetrics(RpcLabels.CLIENT_ID.label, this.clientId)
    val response = remotingClient.invokeSync(addr, request, timeoutMillis)
    if (response.code == ResponseCode.SUCCESS) {
        return GroupList.decode(response.body, GroupList::class.java)
    }

    throw MQBrokerException(response.code, response.remark)
}

fun String.isInteger() = toIntOrNull()?.let { true } ?: false
fun String.isNotInteger() = !isInteger()

fun SubTrace.getGroup(): String {
    if (before?.groupName?.isNotBlank() == true) {
        return before?.groupName!!
    }
    if (after?.groupName?.isNotBlank() == true) {
        return after?.groupName!!
    }
    return "UNKNOWN"
}
