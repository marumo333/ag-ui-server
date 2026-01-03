import express from "express";
import { RunAgentInputSchema, RunAgentInput } from "@ag-ui/core";
 
const app = express();
app.use(express.json());
 
app.post("/awp", async (req, res) => {
  try {
    // Zod スキーマでリクエストボディを検証
    const input: RunAgentInput = RunAgentInputSchema.parse(req.body);
    res.json({
      message: `threadId: ${input.threadId}`,
    });
  } catch (error) {
    res.status(422).json({ error: error.message });
  }
});
 
app.listen(8080, () => {
  console.log("Server is running on http://localhost:8080");
});