//package gfs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;


public class Client {

    public static void main(String[] argv) throws Exception {
        DFS dfs;

        System.out.print("Please input MasterManager server ip: ");
        BufferedReader readBuffer = new BufferedReader(new InputStreamReader(System.in));
        String masterIP = readBuffer.readLine();

        if (masterIP.trim().length() != 0) {
            dfs = new DFS(masterIP + ":9500");
        } else {
            dfs = new DFS("192.168.1.111:9500");
        }

        System.out.println("Please choose test task：");

        System.out.println("1: Upload & Download");
        System.out.println("2: Append");
        System.out.println("3: Deleted File");

        String readnum;
        while ((readnum = readBuffer.readLine()).trim().length() != 0) {
            int num = Integer.parseInt(readnum);
            String fileName;
            switch (num) {
                case 1:
                    fileName = "en.pdf";
                    dfs.upload(fileName);
                    System.out.println("Upload Completed");
                    dfs.download(fileName);
                    System.out.println("Download Completed");

                    break;
                case 2:
                    dfs.upload("test_en.pdf");

                    int n;
                    int defSize = 1024 * 1024 * 100;
                    byte[] buffer = new byte[defSize];
                    byte[] upbytes;
                    String path = "data/client/";
                    InputStream input = new FileInputStream(path + "test_en.append");
                    input.skip(0);
                    n = input.read(buffer, 0, defSize);
                    upbytes = new byte[n];
                    System.arraycopy(buffer, 0, upbytes, 0, n);

                    input.close();
                    dfs.updateFile("test_en.pdf", upbytes);
                    dfs.download("test_en.pdf");
                    System.out.println("Append Completed");

                    break;
                case 3:
                    fileName = "tiger.pdf";
                    dfs.upload(fileName);
                    System.out.println("Upload Completed");
                    dfs.delete(fileName);
                    System.out.println("Delete Completed");

                    break;
            }
        }
    }
}
