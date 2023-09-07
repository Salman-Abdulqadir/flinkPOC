import { Request, Response } from "express";
import { FlinkJobService } from "../service/FlinkJobService";

export class FlinkJobController {
  private flinkJobService: FlinkJobService;

  constructor(flinkService: FlinkJobService) {
    this.flinkJobService = flinkService;
  }
  public submitFlinkJob = async (req: Request, res: Response) => {
    const flinkUrl = req.body.flinkUrl;
    const jarFilePath = req.body.jarfile;
    try {
      await this.flinkJobService.submitJob(flinkUrl, jarFilePath);
      res.sendStatus(200);
    } catch (err: any) {
      res.sendStatus(400);
    }
  };
}
