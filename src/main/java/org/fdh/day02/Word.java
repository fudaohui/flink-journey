package org.fdh.day02;

public class Word {

    public String wordName;
    public int wordCount;

    public Word(String wordName, int wordCount) {
        this.wordName = wordName;
        this.wordCount = wordCount;
    }

    public Word() {

    }

    public String getWordName() {
        return wordName;
    }

    public void setWordName(String wordName) {
        this.wordName = wordName;
    }

    public int getWordCount() {
        return wordCount;
    }

    public void setWordCount(int wordCount) {
        this.wordCount = wordCount;
    }

    @Override
    public String toString() {
        return "Word{" +
                "wordName='" + wordName + '\'' +
                ", wordCount=" + wordCount +
                '}';
    }
}
