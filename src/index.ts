import express from "express";
import { RunAgentInputSchema, RunAgentInput } from "@ag-ui/core";
import { MyAgent } from "./agent";
 
const app = express();
app.use(express.json());
 
app.post("/awp", async (req, res) => {
  try {
    const input: RunAgentInput = RunAgentInputSchema.parse(req.body);
 
    console.log("Received input:", input);
 
    // レスポンスヘッダーを設定
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
 
    const agent = new MyAgent();
 
    // AG-UI エージェントを実行し、ストリーミングイベントを返す
    // NOTE: `agent.run` は protected メソッドであるため本来は直接呼び出すはずではない
    // public メソッドである runAgent を呼び出すべきであるように思われるが、
    // ドキュメントの型定義と実装が一致しておらず Promise<void> を返すため、ここでは代わりに `run` メソッドを直接呼び出す
    const stream = agent.run(input);
 
    // ストリーミングイベントをクライアントに送信
    stream.subscribe({
      next(event) {
        res.write(`data: ${JSON.stringify(event)}\n\n`);
      },
      error(err) {
        console.error("Error in agent run:", err);
        res.write(
          `data: ${JSON.stringify({ type: "error", message: err.message })}\n\n`
        );
        res.end();
      },
      complete() {
        res.end();
      },
    });
  } catch (error) {
    res.status(422).json({ error: error.message });
  }
});

app.listen(8080, () => {
  console.log("Server is running on http://localhost:8080");
});