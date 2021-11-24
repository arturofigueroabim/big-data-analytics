package assignment06;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

// implementation of a generic hash function
// see slide 4-76

public class GenericHashFunction {
	public final static int DEFAULT_P = 99991;
	public static List<Set<Integer>> listShigleDocuments = new ArrayList<Set<Integer>>();
	public static Set<Integer> unionSet = new HashSet<Integer>();
	static String pathFile = "C:\\Users\\Lisandro\\Desktop\\Data Science - Uni\\Big Data\\Assignment 6\\BDA\\data\\output\\outputTask1c.txt";
	static FileWriter myWriter = null;

	long a, b, p, N;

	public GenericHashFunction(int a, int b, int p, int N) {
		this.a = a;
		this.b = b;
		this.p = p;
		this.N = N;
	}

	// convenience method that takes a shingle as a parameter
	public int hash(String shingle) {
		return hash(shingle.hashCode());
	}

	public int hash(long x) {
		// we are repairing negative input values
		if (x < 0)
			x += -((long) Integer.MIN_VALUE);

		return (int) (((a * x + b) % p) % N);
	}

	// generate array with k randomly chosen hash functions
	// note that this will not work with more than 99991 shingles
	// N is the number of shingles
	public static GenericHashFunction[] generate(int k, int N) {
		Random random = new Random(42);

		GenericHashFunction h[] = new GenericHashFunction[k];

		for (int i = 0; i < k; i++) {
			h[i] = new GenericHashFunction(random.nextInt(Integer.MAX_VALUE), random.nextInt(Integer.MAX_VALUE),
					DEFAULT_P, N);
			}

		return h;

	}

	public static void main(String args[]) throws IOException {
		Shingling shingling = new Shingling();
		shingling.createFile(pathFile);
		int[] functions = { 1, 5, 10 };

		myWriter = new FileWriter(pathFile);

		for (int i = 0; i < functions.length; i++) {

			int[][] signatureVector = minHashSignature(functions[i], Task1bSkeleton.documents);

			similaritySignatures(signatureVector);
		}

		System.out.println("Successfully wrote to the file.");
		myWriter.close();

	}

	public static int[][] minHashSignature(int h, String[] shingleArray) {
		int size = shingleArray.length;
		int numHashFuction = h;

		int[][] signatureVector = new int[numHashFuction][size];

		for (int i = 0; i < size; i++) {
			listShigleDocuments.add(computeShingles(shingleArray[i], 5));
			unionSet.addAll(listShigleDocuments.get(i));
		}

		GenericHashFunction[] hi = generate(numHashFuction, unionSet.size());

		
		boolean next = false;
		
		for (int i = 0; i < hi.length;) {

			for (int j = 0; j < size;) {

				for (Integer r : unionSet) {

					for (Integer s : listShigleDocuments.get(j)) {

						if (hi[i].hash(s) == hi[i].hash(r)) {

							signatureVector[i][j] = hi[i].hash(s);

							j++;

							if (j == size) {
								i++;
							}

							next = true;
							break;

						} else {
							next = false;
						}
					}

					if (next)
						break;

				}
				if (next)
					continue;
			}

		}

		return signatureVector;

	}
	
	
	public static int[] minHashSignature(int h, String shingleDocument) {
		int numHashFuction = h;

		int[] signatureVector = new int[numHashFuction];

		Set<Integer> union = new HashSet<Integer>();

		union.addAll(computeShingles(shingleDocument, 5));

		GenericHashFunction[] hi = generate(numHashFuction, 10);

		for (int i = 0; i < hi.length; i++) {

			for (Integer s : union) {

				signatureVector[i] = hi[i].hash(s);
			}
		}
		return signatureVector;
	}
	
	
	public static void similaritySignatures(int[][] signatureVector) throws IOException {

		Set<Integer> iDocument = new HashSet<Integer>();

		int size = signatureVector[0].length;
		int combi = 0;
		int j = 0;

		myWriter.write(String.format(" Similarity of signatures with %d functions \n", signatureVector.length));

		for (int h = 0; h < size; h++) {

			Set<Integer> jDocument = new HashSet<Integer>();

			if (combi == size - 1) {
				break;
			}

			myWriter.write(String.format("S%d: ", j));

			for (int i = 0; i < signatureVector.length; i++) {

				for (; j < signatureVector[i].length;) {

					// System.out.println("i: " + i + " j: " + j + " signatureVector i: " +
					// signatureVector[i][j]);
					// System.out.println(" ");

					myWriter.write(String.format(" %d ", signatureVector[i][j]));

					jDocument.add(signatureVector[i][j]);

					break;
				}
			}
			myWriter.write("\n");

			if (h == combi) {
				iDocument = jDocument;

			} else {

				Set<Integer> unionSet = new HashSet<Integer>(iDocument);
				unionSet.addAll(jDocument);

				Set<Integer> interSet = new HashSet<Integer>(iDocument);
				interSet.retainAll(jDocument);

				double jaccard = (double) interSet.size() / (double) unionSet.size();

				myWriter.write(String.format("sim( %d, %d)= %.2f \n", combi, j, jaccard));
				myWriter.write("\n");

				// System.out.println(String.format("sim( %d, %d)= %.2f \n", combi, j,
				// jaccard));
				// System.out.println("\n");
			}

			if (h == size - 1) {
				combi++;
				j = combi;
				h = combi - 1;
				continue;
			}

			j++;
		}

	}

	public static Set<Integer> computeShingles(String text, int k) {
		String[] arrayShingle = text.replace('\"', ' ').replace('!', ' ').replace('\'', ' ').replace('?', ' ')
				.replace('(', ' ').replace(')', ' ').replace('-', ' ').replace(".", "").replace(':', ' ')
				.replace(';', ' ').replace('=', ' ').replace(",", "").replace('[', ' ').replace(']', ' ').toLowerCase()
				.split(" ");
		Set<Integer> testShingle = new HashSet<Integer>();

		StringBuilder temString = new StringBuilder();
		int count = 0;

		for (int i = 0; i < arrayShingle.length; i++) {

			if (count < k) {
				temString.append(arrayShingle[i]);
				temString.append(" ");
				count++;
			} else {
				testShingle.add(temString.toString().trim().hashCode());
				temString = new StringBuilder();
				i = i - k;
				count = 0;
			}
		}
		testShingle.add(temString.toString().trim().hashCode());
		return testShingle;
	}

}
