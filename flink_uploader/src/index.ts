import express, { Request, Response } from "express";

const app = express();

app.use(express.json());

app.get("/test", (req: Request, res: Response) => {
  res.json("working");
});

app.listen(3000, () => console.log("Listening on port 3000"));
