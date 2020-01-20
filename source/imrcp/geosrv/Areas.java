import java.awt.geom.Area;
import java.awt.geom.Path2D.Float;
import java.awt.geom.PathIterator;


public class Areas
{
	Areas()
	{
	}


	public static void main(String[] sArgs)
	{
		Float[] oPaths = new Float[5];
		
		Float oPath = new Float();
		oPath.moveTo(1.0, 3.0);
		oPath.lineTo(4.0, 3.0);
		oPath.lineTo(4.0, 2.0);
		oPath.lineTo(1.0, 2.0);
		oPath.closePath();
		oPaths[0] = oPath;

		oPath = new Float();
		oPath.moveTo(5.0, 3.0);
		oPath.lineTo(7.0, 3.0);
		oPath.lineTo(7.0, 2.0);
		oPath.lineTo(5.0, 2.0);
		oPath.closePath();
		oPaths[1] = oPath;

		oPath = new Float();
		oPath.moveTo(0.0, 2.0);
		oPath.lineTo(2.0, 2.0);
		oPath.lineTo(2.0, 1.0);
		oPath.lineTo(0.0, 1.0);
		oPath.closePath();
		oPaths[2] = oPath;

		oPath = new Float();
		oPath.moveTo(3.0, 2.0);
		oPath.lineTo(6.0, 2.0);
		oPath.lineTo(6.0, 1.0);
		oPath.lineTo(3.0, 1.0);
		oPath.closePath();
		oPaths[3] = oPath;

		oPath = new Float();
		oPath.moveTo(1.0, 1.0);
		oPath.lineTo(4.0, 1.0);
		oPath.lineTo(4.0, 0.0);
		oPath.lineTo(1.0, 0.0);
		oPath.closePath();
		oPaths[4] = oPath;

		Area oArea = new Area();
		for (Float oAreaPath : oPaths)
			oArea.add(new Area(oAreaPath));

		float[] oCoords = new float[2];
		PathIterator oIt = oArea.getPathIterator(null);
		while (!oIt.isDone())
		{
			while (oIt.currentSegment(oCoords) != PathIterator.SEG_CLOSE)
			{
				System.out.print(oCoords[0]);
				System.out.print(",");
				System.out.println(oCoords[1]);
				oIt.next();
			}
			oIt.next();
			System.out.println();
		}
	}
}
