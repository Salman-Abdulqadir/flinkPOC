import axios from "axios";
import * as fs from "fs";
import FormData from "form-data";

export class FlinkJobService {
  public uploadJar = async (flinkUrl: string, jarFilePath: string) => {
    const formData = new FormData();
    formData.append("jarfile", fs.createReadStream(jarFilePath));

    const response = await axios.post(flinkUrl, formData, {
      headers: {
        Expect: "",
        ...formData.getHeaders(),
      },
    });
    return response.data;
  };
  public submitJob = async (flinkUrl: string, jarFilePath: string) => {
    const path = await this.uploadJar(flinkUrl, jarFilePath);
  };
}
