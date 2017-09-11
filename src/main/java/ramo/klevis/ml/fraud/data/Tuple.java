package ramo.klevis.ml.fraud.data;

import java.io.Serializable;

public class Tuple<F extends Serializable, S extends Serializable> implements Serializable {
    private F label;
    private S value;

    public Tuple(F label, S value) {

        this.label = label;
        this.value = value;
    }

    public F getLabel() {
        return label;
    }

    public S getValue() {
        return value;
    }
}