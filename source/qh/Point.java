/**
    Copyright (c) 2014 Patrick Hillert <silent@gmx.biz>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; version 3 of the License.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package qh;

/**
 * A simple point class wrapper for three dimensions.
 */
public class Point {
    /**
     * The x value.
     */
    private double vX;

    /**
     * The y value.
     */
    private double vY;

    /**
     * The z value.
     */
    private double vZ;

    /**
     * Initializes a new 2D Point.
     *
     * @param x The x value of the point.
     * @param y The y value of the point
     */
    public Point(final double x, final double y) {
        vX = x;
        vY = y;
        vZ = 0;
    }

    /**
     * Initializes a new 3D Point.
     *
     * @param x The x value of the point.
     * @param y The y value of the point
     * @param z The z value of the point
     */
    public Point(final double x, final double y, final double z) {
        vX = x;
        vY = y;
        vZ = z;
    }

    /**
     * Returns the x value.
     *
     * @return The x value.
     */
    public final double getX() {
        return vX;
    }

    /**
     * Returns the y value.
     *
     * @return The y value.
     */
    public final double getY() {
        return vY;
    }

    /**
     * Returns the z value.
     *
     * @return The z value.
     */
    public final double getZ() {
        return vZ;
    }
}
