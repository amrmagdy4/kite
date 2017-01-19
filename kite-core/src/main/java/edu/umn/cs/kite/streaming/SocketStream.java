package edu.umn.cs.kite.streaming;

import edu.umn.cs.kite.datamodel.Scheme;
import edu.umn.cs.kite.preprocessing.Preprocessor;
import edu.umn.cs.kite.util.microblogs.Microblog;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;

/**
 * Created by amr_000 on 8/8/2016.
 */

public  class SocketStream extends StreamingDataSource {
    private String host;
    private Integer port;
    private Preprocessor<String,Microblog> preprocessor;

    private Socket socket = null;

    private BufferedReader in = null;
    private PrintWriter out = null;

    public SocketStream(Scheme scheme, String host, Integer port,
                        Preprocessor<String,Microblog> preprocessor) {
        setScheme(scheme);
        this.host = host;
        this.port = port;
        this.preprocessor = preprocessor;

        try {
            socket = new Socket(host, port);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out = new PrintWriter(socket.getOutputStream(), true);
        } catch (Exception e) {
        }
    }

    public ArrayList<String> getData() throws IOException {
        ArrayList<String> data = new ArrayList<String>();
        String line;
        out.println("data");
        line = in.readLine();
        int batchSize = Integer.parseInt(line);
        for(int i = 0; i < batchSize; ++i) {
            line = in.readLine();
            data.add(line);
        }
        return data;
    }

    @Override
    public Preprocessor<String, Microblog> getPreprocessor() {
        return preprocessor;
    }

    @Override
    public String toString() {
        return host+":"+port;
    }

    public void queueData() throws Exception {
        throw new Exception("Incomplete implementation");
		/*
		String line;
		out.println("data");
		line = in.readLine();
		int batchSize = Integer.parseInt(line);
		for(int i = 0; i < batchSize; ++i) {
			line = in.readLine();
			//add line to queue here
		}
		*/
    }
}
