package cs6240.imagePreprocess;

import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import scala.Tuple2;

public class LoadMultiStack
{
	/*
	 * Read image file as DataInputStream, output list of pairRDD, where each is 
	 * (layer number, Array of pixel on this layer)
	 */
    public static List<Tuple2<Integer, byte[]>> parse(java.io.DataInputStream imageFile, 
    		int xd, 
    		int yd, 
    		int zd) {
    	List<Tuple2<Integer, byte[]>> res = new ArrayList<>();
		try {
//			final File imageFile = checkFile(filename);
			final int xDim = xd;
			final int yDim = yd;
			final int zDim = zd;
			final ImageReader reader = buildReader(imageFile, zDim);

			for (int iz = 0; iz < zDim; iz++) {
				final BufferedImage image = reader.read(iz);
		        final DataBuffer dataBuffer = image.getRaster().getDataBuffer();
		        final byte layerBytes[] = ((DataBufferByte)dataBuffer).getData();
		        byte[] matrix = new byte[yDim * xDim];
				System.arraycopy(layerBytes, 0, matrix, 0, xDim * yDim);
				res.add(new Tuple2<Integer, byte[]>(iz, matrix));
			}
	        
	        return res;
		} catch (final Exception e) {
			System.exit(1);
		}
		
		return res;
	}

    private static File checkFile(final String fileName) throws Exception {
    	final File imageFile = new File(fileName);
    	if (!imageFile.exists() || imageFile.isDirectory()) {
    		throw new Exception ("Image file does not exist: " + fileName);
    	}
    	return imageFile;
    }

    private static ImageReader buildReader(java.io.DataInputStream imageFile, final int zDim) throws Exception {
		final ImageInputStream imgInStream = ImageIO.createImageInputStream(imageFile);
		if (imgInStream == null || imgInStream.length() == 0){
			throw new Exception("Data load error - No input stream.");
		}
		Iterator<ImageReader> iter = ImageIO.getImageReaders(imgInStream);
		if (iter == null || !iter.hasNext()) {
			throw new Exception("Data load error - Image file format not supported by ImageIO.");
		}
		final ImageReader reader = iter.next();
		iter = null;
		reader.setInput(imgInStream);
		int numPages;
		if ((numPages = reader.getNumImages(true)) != zDim) {
			throw new Exception("Data load error - Number of pages mismatch: " + numPages + " expected: " + zDim);
		}
		return reader;
    }
}