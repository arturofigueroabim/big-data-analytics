package assignment06;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Task1bMain {

	static String pathFile = "C:\\Users\\Lisandro\\Desktop\\Data Science - Uni\\Big Data\\Assignment 6\\BDA\\data\\output\\outputTask1b.txt";

	public static void main(String[] args) throws IOException {
		Shingling shingling = new Shingling();
		Set<String> iDocument = new HashSet<String>();
		Set<String> jDocument = new HashSet<String>();
		int size = Task1bSkeleton.documents.length;

		shingling.createFile(pathFile);
		FileWriter myWriter = new FileWriter(pathFile);

		for (int h = 1; h <= 5; h += 2) {

			for (int i = 0; i < size; i++) {

				iDocument = shingling.computeShingles(Task1bSkeleton.documents[i], h);

				for (int j = i + 1; j < size; j++) {

					jDocument = shingling.computeShingles(Task1bSkeleton.documents[j], h);

					Set<String> unionSet = new HashSet<String>(iDocument);
					unionSet.addAll(jDocument);

					Set<String> interSet = new HashSet<String>(iDocument);
					interSet.retainAll(jDocument);
					
					//System.out.println(unionSet);
					//System.out.println(interSet);

					double jaccard = (double) interSet.size() / (double) unionSet.size();

					myWriter.write(String.format("%d - Shingles: \n", h));
					myWriter.write(String.format("sim( %d, %d)= %.2f \n", i, j, jaccard));
					myWriter.write("\n");

				}

			}
		}
		System.out.println("Successfully wrote to the file.");
		myWriter.close();
	}

}
