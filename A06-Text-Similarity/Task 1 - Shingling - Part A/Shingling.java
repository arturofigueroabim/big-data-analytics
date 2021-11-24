package assignment06;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Shingling implements Shingler {

	static String pathFile = "C:\\Users\\Lisandro\\Desktop\\Data Science - Uni\\Big Data\\Assignment 6\\BDA\\data\\output\\outputTask1a.txt";
	static String shingleOne = "Data Science is an upcoming field that combines methods from computer science, mathematics and statistics.";
	static String shingleTwo = "Data Science is an emerging field that brings together methods from statistics, mathematics, and computing.";

	public static void main(String[] args) throws IOException {
		Shingling shingling = new Shingling();

		shingling.createFile(pathFile);

		FileWriter myWriter = new FileWriter(pathFile);

		Set<String> first = shingling.computeShingles(shingleOne, 3);
		Set<String> second = shingling.computeShingles(shingleTwo, 3);

		myWriter.write("First Shingling Input: \n");
		myWriter.write("\n");
		for (String string : first) {
			myWriter.write(string + "\n");
		}
		myWriter.write("\n");

		myWriter.write("Second Shingling Input: \n");
		myWriter.write("\n");
		for (String string : second) {
			myWriter.write(string + "\n");
		}

		System.out.println("Successfully wrote to the file.");
		myWriter.close();
	}

	@Override
	public Set<String> computeShingles(String text, int k) {
		String[] arrayShingle = text.replace('\"', ' ').replace('!', ' ').replace('\'', ' ').replace('?', ' ')
				.replace('(', ' ').replace(')', ' ').replace('-', ' ').replace(".", "").replace(':', ' ')
				.replace(';', ' ').replace('=', ' ').replace(",", "").replace('[', ' ').replace(']', ' ').toLowerCase()
				.split(" ");
		Set<String> testShingle = new HashSet<String>();

		StringBuilder temString = new StringBuilder();
		int count = 0;

		for (int i = 0; i < arrayShingle.length; i++) {

			if (count < k) {
				temString.append(arrayShingle[i]);
				temString.append(" ");
				count++;
			} else {
				testShingle.add(temString.toString().trim());
				temString = new StringBuilder();
				i = i - k;
				count = 0;
			}
		}
		testShingle.add(temString.toString().trim());
		return testShingle;
	}

	public void createFile(String path) {
		try {
			File myObj = new File(path);
			if (myObj.createNewFile()) {
				System.out.println("File created: " + myObj.getName());
			} else {
				System.out.println("File already exists.");
			}
		} catch (IOException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
	}
}
