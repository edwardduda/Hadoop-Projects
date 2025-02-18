package ngram;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Book {
	private String headerText, bodyText, author, year;
	private int ngramCount;

	public Book(String rawText, int ngramCount) {
		this.ngramCount = ngramCount;

		// Split rawText into headerText and bodyText
		// Look for the start and end markers in Project Gutenberg texts
		String startPattern = "\\*\\*\\*\\s*START OF (THIS|THE) PROJECT GUTENBERG EBOOK.*?\\s*\\*\\*\\*";
		String endPattern = "\\*\\*\\*\\s*END OF (THIS|THE) PROJECT GUTENBERG EBOOK.*?\\s*\\*\\*\\*";

		Pattern startPatternCompiled = Pattern.compile(startPattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
		Pattern endPatternCompiled = Pattern.compile(endPattern, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

		Matcher startMatcher = startPatternCompiled.matcher(rawText);
		Matcher endMatcher = endPatternCompiled.matcher(rawText);

		int startIndex = 0;
		int endIndex = rawText.length();

		if (startMatcher.find()) {
			startIndex = startMatcher.end();
		}

		if (endMatcher.find()) {
			endIndex = endMatcher.start();
		}

		this.headerText = rawText.substring(0, startIndex);
		this.bodyText = rawText.substring(startIndex, endIndex);

		// Initialize other class variables
		this.author = parseAuthor(headerText);
		this.year = parseYear(headerText);
		this.bodyText = formatBook(this.bodyText);
	}

	private String parseAuthor(String headerText) {
		// Extract author's last name from headerText
		Pattern authorPattern = Pattern.compile("Author:\\s*(.*)", Pattern.CASE_INSENSITIVE);
		Matcher authorMatcher = authorPattern.matcher(headerText);
		if (authorMatcher.find()) {
			String fullName = authorMatcher.group(1).trim();
			String[] nameParts = fullName.split("\\s+");
			if (nameParts.length > 0) {
				return nameParts[nameParts.length - 1];
			}
		}
		return "Unknown";
	}

	private String parseYear(String headerText) {
		// Extract release year from headerText
		Pattern yearPattern = Pattern.compile("Release Date:.*?(\\d{4})", Pattern.CASE_INSENSITIVE);
		Matcher yearMatcher = yearPattern.matcher(headerText);
		if (yearMatcher.find()) {
			return yearMatcher.group(1);
		}
		return "Unknown";
	}

	public String getBookAuthor() {
		return author;
	}

	public String getBookYear() {
		return year;
	}

	public String getBookHeader() {
		return headerText;
	}

	public String getBookBody() {
		return bodyText;
	}

	private String formatBook(String bookText) {
		if (ngramCount < 2) {
			// Format book text for unigram
			// Convert all text to lowercase
			String formattedText = bookText.toLowerCase();

			// Remove apostrophes
			formattedText = formattedText.replaceAll("'", "");

			// Remove hyphens not between word characters
			formattedText = formattedText.replaceAll("(?<!\\w)-(?!\\w)", " ");

			// Remove all other punctuation except hyphens
			formattedText = formattedText.replaceAll("[\\p{Punct}&&[^-]]+", " ");

			// Replace multiple spaces with single space
			formattedText = formattedText.replaceAll("\\s+", " ").trim();

			return formattedText;
		} else {
			// Format book text for bigram
			// Identify sentence boundaries before removing punctuation
			String[] sentences = bookText.split("(?<=[.!?])\\s+");

			StringBuilder formattedText = new StringBuilder();

			for (String sentence : sentences) {
				// Convert to lowercase
				sentence = sentence.toLowerCase();

				// Remove apostrophes
				sentence = sentence.replaceAll("'", "");

				// Remove hyphens not between word characters
				sentence = sentence.replaceAll("(?<!\\w)-(?!\\w)", " ");

				// Remove all other punctuation except hyphens
				sentence = sentence.replaceAll("[\\p{Punct}&&[^-]]+", " ");

				// Replace multiple spaces with single space
				sentence = sentence.replaceAll("\\s+", " ").trim();

				if (!sentence.isEmpty()) {
					// Add _START_ and _END_ tokens
					formattedText.append("_START_ ").append(sentence).append(" _END_ ");
				}
			}

			return formattedText.toString().trim();
		}
	}
}
