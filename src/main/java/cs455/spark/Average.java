package cs455.spark;

import java.io.Serializable;

public class Average implements Serializable {

    private static final long serialVersionUID = 1L;

    private double sum;
    private double num;

    Average(double sum, double num) {
        this.sum = sum;
        this.num = num;
    }

    public double getAverage() { return this.sum / this.num; }

    double getSum() { return this.sum; }

    double getNum() { return this.num; }
}