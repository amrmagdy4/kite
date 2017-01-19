package edu.umn.cs.kite.indexing.memory.spatial;

import edu.umn.cs.kite.util.GeoLocation;
import edu.umn.cs.kite.util.PointLocation;
import edu.umn.cs.kite.util.Point_2DCartesian;
import edu.umn.cs.kite.util.Rectangle;
import edu.umn.cs.kite.util.serialization.ByteStream;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by amr_000 on 8/4/2016.
 */
public class GridPartitioner implements SpatialPartitioner {

    private Rectangle gridBoundaries;
    private int numRows, numColumns;
    private double gridCellWidth, gridCellHeight;

    public ByteStream serialize() {
        int serializationLen = Byte.BYTES+Rectangle.numBytes()+2*Integer
                .BYTES;
        int capacity = Integer.BYTES+serializationLen;
        ByteStream serializedByte = new ByteStream(capacity);
        byte partitionerType = 0;//0 means grid partitioner
        serializedByte.write(serializationLen);
        serializedByte.write(partitionerType);
        gridBoundaries.serialize(serializedByte);
        serializedByte.write(numRows);
        serializedByte.write(numColumns);
        return serializedByte;
    }

    @Override
    public boolean contained(Integer partitionId, Rectangle range) {
        Point_2DCartesian xy = xy(partitionId);
        double partitionNorth = gridBoundaries.getNorth()-
                                xy.getY()*gridCellHeight;
        double partitionSouth = partitionNorth-gridCellHeight;
        double partitionWest = gridBoundaries.getWest() + xy.getX()
                *gridCellWidth;
        double partitionEast = partitionWest + gridCellWidth;

        return range.isInside(new Rectangle(partitionNorth, partitionSouth,
                partitionEast, partitionWest));
    }

    public GridPartitioner(Rectangle mbr, int rows, int columns) {
        this.gridBoundaries = mbr;
        this.numColumns = columns;
        this.numRows = rows;
        this.gridCellWidth = mbr.getWidth() / numColumns;
        this.gridCellHeight = mbr.getHeight() / numRows;
    }

    @Override
    public List<Integer> overlap(GeoLocation location) {
        List<Integer> partitionIds = null;

        if(location.getClass().getName().compareTo(PointLocation.class
                .getName())==0)
        {
            //overlap predicate for point comes as "inside" predicate
            //this gets at most one partition
            PointLocation point = (PointLocation) location;
            partitionIds = overlap(point);
        }
        else if(location.getClass().getName().compareTo(Rectangle.class
                    .getName())==0) {
            Rectangle rectangle = (Rectangle) location;
            partitionIds = overlap(rectangle);
        }
        return partitionIds;
    }

    private List<Integer> overlap(PointLocation point) {
        List<Integer> partitionId = new ArrayList<>();

        if(gridBoundaries.isInside(point)) {
            Point_2DCartesian point_xy = gridXY(point);
            Integer pId = partitionId(point_xy.getX(), point_xy.getY());
            partitionId.add(pId);
        }
        return partitionId;
    }

    private List<Integer> overlap(Rectangle rectangle) {
        List<Integer> partitionIds = new ArrayList<>();

        if(!rectangle.isValid()) {
            throw new IllegalArgumentException("Invalid rectangular space " +
                    "boundaries" + System.lineSeparator() + "North=" + rectangle
                    .getNorth() + ", " + "South=" + rectangle
                    .getSouth() + ", " + "East=" + rectangle
                    .getEast() + ", " + "West=" + rectangle
                    .getWest());
            //ToDo: Log exception info
        }

        PointLocation nwPoint = new PointLocation(rectangle.getNorth(),
                rectangle.getWest());
        PointLocation nePoint = new PointLocation(rectangle.getNorth(),
                rectangle.getEast());
        PointLocation sePoint = new PointLocation(rectangle.getSouth(),
                rectangle.getEast());
        PointLocation swPoint = new PointLocation(rectangle.getSouth(),
                rectangle.getWest());

        boolean overlaps = gridBoundaries.isInside(nwPoint)
                        || gridBoundaries.isInside(nePoint)
                        || gridBoundaries.isInside(sePoint)
                        || gridBoundaries.isInside(swPoint);
        //if all points outside the grid, no overlap
        if(!overlaps) return new ArrayList<>(); //return 0 overlap partitions

        Point_2DCartesian p1 = gridXY(nwPoint);
        Point_2DCartesian p2 = gridXY(swPoint);
        Point_2DCartesian p3 = gridXY(nePoint);
        //Point_2DCartesian seXY = gridXY(sePoint);

        //if any point outside the grid, snap it to grid boundaries
        //nw
        if(p1.getX() < 0) p1.setX(0);
        if(p1.getX() >= numColumns) p1.setX(numColumns-1);
        if(p1.getY() < 0) p1.setY(0);
        if(p1.getY() >= numRows) p1.setY(numRows-1);
        //sw
        if(p2.getX() < 0) p2.setX(0);
        if(p2.getX() >= numColumns) p2.setX(numColumns-1);
        if(p2.getY() < 0) p2.setY(0);
        if(p2.getY() >= numRows) p2.setY(numRows-1);
        //ne
        if(p3.getX() < 0) p3.setX(0);
        if(p3.getX() >= numColumns) p3.setX(numColumns-1);
        if(p3.getY() < 0) p3.setY(0);
        if(p3.getY() >= numRows) p3.setY(numRows-1);

        int rowCount = p2.getY()-p1.getY()+1;
        int colCount = p3.getX()-p1.getX()+1;
        for(int i = 0; i < rowCount; ++i)
        {
            int y = p1.getY() + i;
            for(int j = 0; j < colCount; ++j)
            {
                int x = p1.getX() + j;
                partitionIds.add(partitionId(x,y));
            }
        }
        return partitionIds;
    }

    private Integer partitionId(int x, int y) {
        return numColumns*y + x;
        /* This formula gives partition ids as follows for 6x5 grid
                --------- North ---------
                |   0   1   2   3   4   |
                W   5   6   7   8   9   E
                e   10  11  12  13  14  a
                s   15  16  17  18  19  s
                t   20  21  22  23  24  t
                |   25  26  27  28  29  |
                --------- South ---------
         */
    }

    private Point_2DCartesian xy(Integer partitionId) {
        if(partitionId < 0 || partitionId > maxPartitionId())
            return null;
        int y = (int)(partitionId/numColumns);
        int x = partitionId-y*numColumns;
        Point_2DCartesian xy = new Point_2DCartesian(x,y);
        return xy;
    }

    private Integer maxPartitionId() {
        return numRows*numColumns-1;
    }

    private Point_2DCartesian gridXY(PointLocation point) {
        //The assumption here that grid origin, the (0,0) coordinates, is
        // north-west corner
        double pointWidth = point.getLongitude() - gridBoundaries
                .getWest();
        double pointHeight = gridBoundaries
                .getNorth() - point.getLatitude();
        int point_x = (int)Math.floor(pointWidth/gridCellWidth);
        int point_y = (int)Math.floor(pointHeight/gridCellHeight);
        return new Point_2DCartesian(point_x, point_y);
    }
}
