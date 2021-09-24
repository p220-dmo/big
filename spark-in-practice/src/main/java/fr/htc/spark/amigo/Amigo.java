package fr.htc.spark.amigo;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

public class Amigo {

	private int numTirage;
	private LocalDate dateTirage;
	private LocalTime heureTirage;
	private LocalDate dateForclusion;
	private int bleu1;
	private int bleu2;
	private int bleu3;
	private int bleu4;
	private int bleu5;
	private int bleu6;
	private int bleu7;
	private int bonus1;
	private int bonus2;
	private int bonus3;
	private int bonus4;
	private int bonus5;

	/**
	 * 
	 * @param line
	 * @return
	 */
	public static Amigo parse(String line) {
		System.out.println(line);
		String[] columns = line.split(";");
		Amigo amigo = new Amigo();

		amigo.setNumTirage(Integer.parseInt(columns[0]));
		amigo.setDateTirage(LocalDate.parse(columns[1]));
		amigo.setHeureTirage(LocalTime.parse(columns[2]));
		amigo.setDateForclusion(LocalDate.parse(columns[3]));
		amigo.setBleu1(Integer.parseInt(columns[4]));
		amigo.setBleu2(Integer.parseInt(columns[5]));
		amigo.setBleu3(Integer.parseInt(columns[6]));
		amigo.setBleu4(Integer.parseInt(columns[7]));
		amigo.setBleu5(Integer.parseInt(columns[8]));
		amigo.setBleu6(Integer.parseInt(columns[9]));
		amigo.setBleu7(Integer.parseInt(columns[10]));
		amigo.setBonus1(Integer.parseInt(columns[11]));
		amigo.setBonus2(Integer.parseInt(columns[12]));
		amigo.setBonus3(Integer.parseInt(columns[13]));
		amigo.setBonus4(Integer.parseInt(columns[14]));
		amigo.setBonus5(Integer.parseInt(columns[15]));
		
		return amigo;
	}
	
	public List<Integer> getBleus(){
		List<Integer> bleus = new ArrayList<Integer>();
		
		bleus.add(bleu1);
		bleus.add(bleu2);
		bleus.add(bleu3);
		bleus.add(bleu4);
		bleus.add(bleu5);
		bleus.add(bleu6);
		bleus.add(bleu7);
		
		return bleus;
	}
	
	public int getNumTirage() {
		return numTirage;
	}

	public void setNumTirage(int numTirage) {
		this.numTirage = numTirage;
	}

	public LocalDate getDateTirage() {
		return dateTirage;
	}

	public void setDateTirage(LocalDate dateTirage) {
		this.dateTirage = dateTirage;
	}

	public LocalTime getHeureTirage() {
		return heureTirage;
	}

	public void setHeureTirage(LocalTime heureTirage) {
		this.heureTirage = heureTirage;
	}


	public LocalDate getDateForclusion() {
		return dateForclusion;
	}

	public void setDateForclusion(LocalDate dateForclusion) {
		this.dateForclusion = dateForclusion;
	}

	public int getBleu1() {
		return bleu1;
	}

	public void setBleu1(int bleu1) {
		this.bleu1 = bleu1;
	}

	public int getBleu2() {
		return bleu2;
	}

	public void setBleu2(int bleu2) {
		this.bleu2 = bleu2;
	}

	public int getBleu3() {
		return bleu3;
	}

	public void setBleu3(int bleu3) {
		this.bleu3 = bleu3;
	}

	public int getBleu4() {
		return bleu4;
	}

	public void setBleu4(int bleu4) {
		this.bleu4 = bleu4;
	}

	public int getBleu5() {
		return bleu5;
	}

	public void setBleu5(int bleu5) {
		this.bleu5 = bleu5;
	}

	public int getBleu6() {
		return bleu6;
	}

	public void setBleu6(int bleu6) {
		this.bleu6 = bleu6;
	}

	public int getBleu7() {
		return bleu7;
	}

	public void setBleu7(int bleu7) {
		this.bleu7 = bleu7;
	}

	public int getBonus1() {
		return bonus1;
	}

	public void setBonus1(int bonus1) {
		this.bonus1 = bonus1;
	}

	public int getBonus2() {
		return bonus2;
	}

	public void setBonus2(int bonus2) {
		this.bonus2 = bonus2;
	}

	public int getBonus3() {
		return bonus3;
	}

	public void setBonus3(int bonus3) {
		this.bonus3 = bonus3;
	}

	public int getBonus4() {
		return bonus4;
	}

	public void setBonus4(int bonus4) {
		this.bonus4 = bonus4;
	}

	public int getBonus5() {
		return bonus5;
	}

	public void setBonus5(int bonus5) {
		this.bonus5 = bonus5;
	}

	@Override
	public String toString() {
		return "Amigo [numTirage=" + numTirage + ", bleu1=" + bleu1 + ", bleu2=" + bleu2 + ", bleu3=" + bleu3
				+ ", bleu4=" + bleu4 + ", bleu5=" + bleu5 + ", bleu6=" + bleu6 + ", bleu7=" + bleu7 + ", bonus1="
				+ bonus1 + ", bonus2=" + bonus2 + ", bonus3=" + bonus3 + ", bonus4=" + bonus4 + ", bonus5=" + bonus5
				+ "]";
	}
}
