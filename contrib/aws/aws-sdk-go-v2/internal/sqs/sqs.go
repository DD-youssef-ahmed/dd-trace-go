// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package sqs

import (
	"context"
	"encoding/json"

	"github.com/DataDog/dd-trace-go/contrib/aws/aws-sdk-go-v2/v2/internal"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go/middleware"

	"github.com/DataDog/dd-trace-go/v2/datastreams"
	"github.com/DataDog/dd-trace-go/v2/datastreams/options"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
)

const (
	datadogKey           = "_datadog"
	maxMessageAttributes = 10
)

var instr = internal.Instr

// EnrichOperation runs before the SQS API call. It tags the span with
// messaging.system, injects the trace context into outgoing messages, and
// (when DSM is enabled) sets a Data Streams Monitoring produce checkpoint
// for SendMessage/SendMessageBatch. For ReceiveMessage, it ensures the
// _datadog message attribute will be returned in the response so the
// consume checkpoint can be set in EnrichOperationOutput.
func EnrichOperation(ctx context.Context, span *tracer.Span, in middleware.InitializeInput, operation string, dsmEnabled bool, queueName string) {
	switch operation {
	case "SendMessage":
		handleSendMessage(ctx, span, in, dsmEnabled, queueName)
	case "SendMessageBatch":
		handleSendMessageBatch(ctx, span, in, dsmEnabled, queueName)
	case "ReceiveMessage":
		if dsmEnabled {
			ensureDatadogAttributeRequested(in)
		}
	}
	span.SetTag(ext.MessagingSystem, ext.MessagingSystemSQS)
}

// EnrichOperationOutput runs after the SQS API call and processes the
// response. For ReceiveMessage with DSM enabled, it iterates over the
// returned messages and sets a Data Streams Monitoring consume checkpoint
// for each, using the pathway extracted from the message attributes.
func EnrichOperationOutput(out middleware.InitializeOutput, operation string, dsmEnabled bool, queueName string) {
	if !dsmEnabled {
		return
	}
	if operation != "ReceiveMessage" {
		return
	}
	resp, ok := out.Result.(*sqs.ReceiveMessageOutput)
	if !ok || resp == nil {
		return
	}
	for i := range resp.Messages {
		setConsumeCheckpoint(&resp.Messages[i], queueName)
	}
}

func handleSendMessage(ctx context.Context, span *tracer.Span, in middleware.InitializeInput, dsmEnabled bool, queueName string) {
	params, ok := in.Parameters.(*sqs.SendMessageInput)
	if !ok {
		instr.Logger().Debug("Unable to read SendMessage params")
		return
	}

	carrier := tracer.TextMapCarrier{}
	if err := tracer.Inject(span.Context(), carrier); err != nil {
		instr.Logger().Debug("Unable to inject trace context: %s", err.Error())
		return
	}

	if dsmEnabled {
		// If the outgoing message already carries an upstream pathway (e.g. a
		// forwarder copied the _datadog attribute through from a consumed
		// message), chain the produce checkpoint from it so DSM sees a
		// continuous pathway across the queue boundary.
		parentCtx := parentCtxFromAttributes(ctx, params.MessageAttributes)
		injectProduceCheckpoint(parentCtx, queueName, payloadSize(params), carrier)
	}

	traceContext, err := encodeCarrier(carrier)
	if err != nil {
		instr.Logger().Debug("Unable to encode trace context: %s", err.Error())
		return
	}

	if params.MessageAttributes == nil {
		params.MessageAttributes = make(map[string]types.MessageAttributeValue)
	}
	injectTraceContext(traceContext, params.MessageAttributes)
}

func handleSendMessageBatch(ctx context.Context, span *tracer.Span, in middleware.InitializeInput, dsmEnabled bool, queueName string) {
	params, ok := in.Parameters.(*sqs.SendMessageBatchInput)
	if !ok {
		instr.Logger().Debug("Unable to read SendMessageBatch params")
		return
	}

	for i := range params.Entries {
		carrier := tracer.TextMapCarrier{}
		if err := tracer.Inject(span.Context(), carrier); err != nil {
			instr.Logger().Debug("Unable to inject trace context: %s", err.Error())
			return
		}
		if dsmEnabled {
			parentCtx := parentCtxFromAttributes(ctx, params.Entries[i].MessageAttributes)
			injectProduceCheckpoint(parentCtx, queueName, batchEntrySize(&params.Entries[i]), carrier)
		}
		traceContext, err := encodeCarrier(carrier)
		if err != nil {
			instr.Logger().Debug("Unable to encode trace context: %s", err.Error())
			return
		}
		if params.Entries[i].MessageAttributes == nil {
			params.Entries[i].MessageAttributes = make(map[string]types.MessageAttributeValue)
		}
		injectTraceContext(traceContext, params.Entries[i].MessageAttributes)
	}
}

// parentCtxFromAttributes returns ctx with any pathway carried in the
// _datadog message attribute extracted onto it. This is what lets a
// produce checkpoint chain from an upstream consume when the caller
// forwards the message attributes through.
func parentCtxFromAttributes(ctx context.Context, attrs map[string]types.MessageAttributeValue) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	carrier := readDatadogCarrier(attrs)
	if carrier == nil {
		return ctx
	}
	return datastreams.ExtractFromBase64Carrier(ctx, carrier)
}

// injectProduceCheckpoint sets a produce DSM checkpoint and writes the new
// pathway into the provided carrier so it travels with the outgoing message.
func injectProduceCheckpoint(ctx context.Context, queueName string, payloadBytes int64, carrier tracer.TextMapCarrier) {
	edges := []string{"direction:out", "topic:" + queueName, "type:sqs"}
	if ctx == nil {
		ctx = context.Background()
	}
	newCtx, ok := tracer.SetDataStreamsCheckpointWithParams(ctx, options.CheckpointParams{PayloadSize: payloadBytes}, edges...)
	if !ok {
		return
	}
	datastreams.InjectToBase64Carrier(newCtx, carrier)
}

// setConsumeCheckpoint extracts the producer pathway from msg's attributes
// (if any), sets the in-direction DSM checkpoint, and writes the new
// pathway back into the message attributes so downstream user code can
// continue the pathway from the consume position.
func setConsumeCheckpoint(msg *types.Message, queueName string) {
	if msg == nil {
		return
	}
	carrier := readDatadogCarrier(msg.MessageAttributes)
	edges := []string{"direction:in", "topic:" + queueName, "type:sqs"}
	parentCtx := datastreams.ExtractFromBase64Carrier(context.Background(), carrier)
	newCtx, ok := tracer.SetDataStreamsCheckpointWithParams(parentCtx, options.CheckpointParams{PayloadSize: messageSize(msg)}, edges...)
	if !ok {
		return
	}
	if carrier == nil {
		carrier = tracer.TextMapCarrier{}
	}
	datastreams.InjectToBase64Carrier(newCtx, carrier)
	writeDatadogCarrier(msg, carrier)
}

// ensureDatadogAttributeRequested guarantees the SQS server returns the
// _datadog message attribute (and trace headers, if requested via "All").
// SQS does not return message attributes by default; the user must list
// them explicitly. We append _datadog so DSM/trace context propagation
// works without requiring the user to opt in.
func ensureDatadogAttributeRequested(in middleware.InitializeInput) {
	params, ok := in.Parameters.(*sqs.ReceiveMessageInput)
	if !ok {
		return
	}
	for _, name := range params.MessageAttributeNames {
		if name == "All" || name == ".*" || name == datadogKey {
			return
		}
	}
	params.MessageAttributeNames = append(params.MessageAttributeNames, datadogKey)
}

func readDatadogCarrier(attrs map[string]types.MessageAttributeValue) tracer.TextMapCarrier {
	if attrs == nil {
		return nil
	}
	attr, ok := attrs[datadogKey]
	if !ok {
		return nil
	}
	var raw []byte
	if attr.StringValue != nil {
		raw = []byte(*attr.StringValue)
	} else if len(attr.BinaryValue) > 0 {
		raw = attr.BinaryValue
	}
	if len(raw) == 0 {
		return nil
	}
	carrier := tracer.TextMapCarrier{}
	if err := json.Unmarshal(raw, &carrier); err != nil {
		instr.Logger().Debug("Unable to decode _datadog message attribute: %s", err.Error())
		return nil
	}
	return carrier
}

func writeDatadogCarrier(msg *types.Message, carrier tracer.TextMapCarrier) {
	if msg == nil || len(carrier) == 0 {
		return
	}
	jsonBytes, err := json.Marshal(carrier)
	if err != nil {
		instr.Logger().Debug("Unable to encode _datadog message attribute: %s", err.Error())
		return
	}
	if msg.MessageAttributes == nil {
		msg.MessageAttributes = make(map[string]types.MessageAttributeValue)
	}
	msg.MessageAttributes[datadogKey] = types.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(string(jsonBytes)),
	}
}

func encodeCarrier(carrier tracer.TextMapCarrier) (types.MessageAttributeValue, error) {
	jsonBytes, err := json.Marshal(carrier)
	if err != nil {
		return types.MessageAttributeValue{}, err
	}
	return types.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(string(jsonBytes)),
	}, nil
}

func injectTraceContext(traceContext types.MessageAttributeValue, messageAttributes map[string]types.MessageAttributeValue) {
	// SQS only allows a maximum of 10 message attributes.
	// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-message-attributes
	// Only inject if there's room.
	if len(messageAttributes) >= maxMessageAttributes {
		instr.Logger().Info("Cannot inject trace context: message already has maximum allowed attributes")
		return
	}

	messageAttributes[datadogKey] = traceContext
}

func payloadSize(params *sqs.SendMessageInput) int64 {
	var size int64
	if params.MessageBody != nil {
		size += int64(len(*params.MessageBody))
	}
	for k, v := range params.MessageAttributes {
		size += int64(len(k))
		if v.DataType != nil {
			size += int64(len(*v.DataType))
		}
		if v.StringValue != nil {
			size += int64(len(*v.StringValue))
		}
		size += int64(len(v.BinaryValue))
	}
	return size
}

func batchEntrySize(entry *types.SendMessageBatchRequestEntry) int64 {
	var size int64
	if entry.MessageBody != nil {
		size += int64(len(*entry.MessageBody))
	}
	for k, v := range entry.MessageAttributes {
		size += int64(len(k))
		if v.DataType != nil {
			size += int64(len(*v.DataType))
		}
		if v.StringValue != nil {
			size += int64(len(*v.StringValue))
		}
		size += int64(len(v.BinaryValue))
	}
	return size
}

func messageSize(msg *types.Message) int64 {
	var size int64
	if msg.Body != nil {
		size += int64(len(*msg.Body))
	}
	for k, v := range msg.MessageAttributes {
		size += int64(len(k))
		if v.DataType != nil {
			size += int64(len(*v.DataType))
		}
		if v.StringValue != nil {
			size += int64(len(*v.StringValue))
		}
		size += int64(len(v.BinaryValue))
	}
	return size
}
