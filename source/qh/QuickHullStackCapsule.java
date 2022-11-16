package qh;

import java.util.ArrayList;

public class QuickHullStackCapsule {
    private ArrayList<Point> pointList;
    private Point firstPoint;
    private Point secondPoint;

    public QuickHullStackCapsule(final ArrayList<Point> points,
            final Point first, final Point second) {
        pointList = points;
        firstPoint = first;
        secondPoint = second;
    }

    public ArrayList<Point> getPointList() {
        return pointList;
    }

    public Point getFirstPoint() {
        return firstPoint;
    }

    public Point getSecondPoint() {
        return secondPoint;
    }
}
