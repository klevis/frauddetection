package ramo.klevis.ml.fraud;

/**
 * Created by klevis.ramo on 9/10/2017.
 */
public enum TransactionType {

    ALL(0),
    PAYMENT(1),
    TRANSFER(2),
    CASH_OUT(3),
    DEBIT(4),
    CASH_IN(5);

    private int transactionType;

    TransactionType(int transactionType) {

        this.transactionType = transactionType;
    }

    public double getTransactionType() {
        return transactionType;
    }
}
