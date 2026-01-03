import {
    AbstractAgent,
    EventType,
    BaseEvent,
    Message,
    AssistantMessage,
    RunAgentInput,
    TextMessageContentEvent,
    MessagesSnapshotEvent,
    RunFinishedEvent,
    RunStartedEvent,
    ToolCallArgsEvent,
    ToolCallEndEvent,
    ToolCallStartEvent,
    ToolCall,
    ToolMessage,
    RunErrorEvent,
    TextMessageStartEvent,
    TextMessageEndEvent,
  } from "@ag-ui/client";
  import { Observable } from "rxjs";
  import {
    CoreMessage,
    processDataStream,
    streamText,
    tool as createVercelAISDKTool,
    ToolSet,
  } from "ai";
  import { anthropic } from "@ai-sdk/anthropic";
  import { randomUUID } from "crypto";
  import { z } from "zod";
   
  export class MyAgent extends AbstractAgent {
    constructor(config = {}) {
      super(config);
    }
    protected run(input: RunAgentInput): Observable<BaseEvent> {
      const finalMessages: Message[] = input.messages;
   
      // rxjs の Observable を使用して、非同期処理を行う
      return new Observable<BaseEvent>((subscriber) => {
        subscriber.next({
          // AG-UI で定義されているイベントタイプを使用して、イベントを発行する
          // RUN_STARTED イベントを発行して、エージェントの実行が開始されたことを通知する
          type: EventType.RUN_STARTED,
          threadId: input.threadId,
          runId: input.runId,
        } as RunStartedEvent);
   
        const response = streamText({
          // Anthropic の Claude 3.5 Haiku モデルを呼び出す
          // Vercel AI SDK は AI モデルを自由に切り替えられるように設計されている
          model: anthropic("claude-3-5-haiku-latest"),
          // HTTP リクエストで受け取った受け取ったメッセージとツールを Vercel AI SDK の形式に変換してから設定する
          messages: convertMessagesToVercelAISDKMessages(input.messages),
          tools: convertToolToVerlAISDKTools(input.tools),
          maxSteps: 5,
        });
   
        const messageId = randomUUID();
        // メッセージが開始されたことを通知する
        const startEvent: TextMessageStartEvent = {
          type: EventType.TEXT_MESSAGE_START,
          messageId,
          role: "assistant",
        };
   
        subscriber.next(startEvent);
   
        let assistantMessage: AssistantMessage = {
          id: messageId,
          role: "assistant",
          content: "",
          toolCalls: [],
        };
        finalMessages.push(assistantMessage);
   
        // processDataStream はストリーミングレスポンスを処理するための AI SDK の関数
        processDataStream({
          stream: response.toDataStreamResponse().body!,
          // テキストのチャンクが到着したときに呼び出されるコールバック関数
          onTextPart: (text) => {
            assistantMessage.content += text;
            subscriber.next({
              // ストリーミングテキストのコンテンツのチャンクを表す
              type: EventType.TEXT_MESSAGE_CONTENT,
              messageId,
              // delta はメッセージのチャンク
              delta: text,
            } as TextMessageContentEvent);
          },
          // メッセージの応答が完了したときに呼び出されるコールバック関数
          onFinishMessagePart: () => {
            // AI モデルの応答が完了したときに発行される
            const event: TextMessageEndEvent = {
              type: EventType.TEXT_MESSAGE_END,
              messageId,
            };
            subscriber.next(event);
   
            // エージェントの実行が完了したことを通知する
            subscriber.next({
              type: EventType.RUN_FINISHED,
              threadId: input.threadId,
              runId: input.runId,
            } as RunFinishedEvent);
   
            subscriber.complete();
          },
          // ツールの呼び出しが要求されたときに呼び出されるコールバック関数
          onToolCallPart(streamPart) {
            let toolCall: ToolCall = {
              id: streamPart.toolCallId,
              type: "function",
              function: {
                name: streamPart.toolName,
                arguments: JSON.stringify(streamPart.args),
              },
            };
            assistantMessage.toolCalls!.push(toolCall);
   
            // ツールの呼び出しが開始されたことを通知するイベントを発行する
            const startEvent: ToolCallStartEvent = {
              type: EventType.TOOL_CALL_START,
              parentMessageId: messageId,
              toolCallId: streamPart.toolCallId,
              toolCallName: streamPart.toolName,
            };
            subscriber.next(startEvent);
   
            // ツール呼び出しの引数データのチャンクを表す
            const argsEvent: ToolCallArgsEvent = {
              type: EventType.TOOL_CALL_ARGS,
              toolCallId: streamPart.toolCallId,
              delta: JSON.stringify(streamPart.args),
            };
            subscriber.next(argsEvent);
   
            // ツールの呼び出しが終了したことを通知するイベントを発行する
            const endEvent: ToolCallEndEvent = {
              type: EventType.TOOL_CALL_END,
              toolCallId: streamPart.toolCallId,
            };
            subscriber.next(endEvent);
          },
          // ツールの結果が返されたときに呼び出されるコールバック関数
          onToolResultPart(streamPart) {
            const toolMessage: ToolMessage = {
              role: "tool",
              id: randomUUID(),
              toolCallId: streamPart.toolCallId,
              content: JSON.stringify(streamPart.result),
            };
            finalMessages.push(toolMessage);
          },
          // ストリーミング中にエラーが発生した場合に呼び出されるコールバック関数
          onErrorPart(streamPart) {
            const runErrorEvent: RunErrorEvent = {
              type: EventType.RUN_ERROR,
              message: "An error occurred during the run",
            };
            subscriber.error(runErrorEvent);
          },
        }).catch((error) => {
          console.error("catch error", error);
          const runErrorEvent: RunErrorEvent = {
            type: EventType.RUN_ERROR,
            message: error.message,
            code: error.code,
          };
          // Handle error
          subscriber.error(runErrorEvent);
        });
   
        return () => {};
      });
    }
  }