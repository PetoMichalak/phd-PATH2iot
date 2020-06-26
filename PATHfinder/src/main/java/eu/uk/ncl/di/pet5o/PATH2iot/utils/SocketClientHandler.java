package eu.uk.ncl.di.pet5o.PATH2iot.utils;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.util.Locale;
import java.util.Scanner;

/**
 * Serves to connect to deployer and transfer the configuration files over.
 */
public class SocketClientHandler {

    private static Logger logger = LogManager.getLogger(SocketClientHandler.class);

    private String ip;
    private int port;
    private Socket socket;
    private ObjectOutputStream oos;
    private ObjectInputStream ois;
    private Scanner scanner;
    private PrintWriter out;

    public SocketClientHandler(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    /**
     * Established connection to the socket server.
     */
    public void connect() {
        // establish connection
        boolean establishingConnection = true;
        while(establishingConnection) {
            try {
                socket = new Socket(ip, port);
                InputStream is = socket.getInputStream();
                scanner = new Scanner(new BufferedInputStream(is));
                scanner.useLocale(Locale.UK);

                OutputStream os = socket.getOutputStream();
                OutputStreamWriter osw = new OutputStreamWriter(os);
                out = new PrintWriter(osw, true);
//                ois = new ObjectInputStream(socket.getInputStream());
//                oos = new ObjectOutputStream(socket.getOutputStream());
                logger.debug(String.format("Connection established %s:%d", ip, port));
                establishingConnection = false;
            } catch (IOException | NullPointerException e) {
                logger.error(String.format("Waiting ... Connection to %s:%d failed. %s", ip, port, e.getMessage()));
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    logger.error("Error sleeping: " + e.getMessage());
                }
            }
        }
    }

    public String readLine() {
        return scanner.nextLine();
    }

    public void printLine(String msg) {
        out.println(msg);
    }

    /**
     * sends the message over the socket connection
     */
    public void send(Object msg) throws IOException {
        oos.writeObject(msg);
        oos.flush();
    }

    public void send(String msg) throws IOException {
        oos.writeUTF(msg);
        oos.flush();
    }

    /**
     * Closes socket and output buffer.
     */
    public void close() throws IOException {
        // following internal protocol, the server shuts down connection upon receiving "EOF"
        send("EOF");
        socket.close();
        oos.close();
    }

    public int readInt() throws IOException {
        return ois.readInt();
    }

    public String readUTF() throws IOException {
        return ois.readUTF();
    }
}
