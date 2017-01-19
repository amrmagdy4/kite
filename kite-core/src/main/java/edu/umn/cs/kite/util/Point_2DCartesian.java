package edu.umn.cs.kite.util;

/**
 * Created by amr_000 on 8/5/2016.
 */
public class Point_2DCartesian {
    private int x,y;

    public Point_2DCartesian(){}

    public Point_2DCartesian(int x, int y) {
        this.x = x;
        this.y = y;
    }

    public void setX(int x) {
        this.x = x;
    }

    public void setY(int y) {
        this.y = y;
    }

    public int getX() {

        return x;
    }

    public int getY() {
        return y;
    }
}
