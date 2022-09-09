package application;

public class Order {
    private String customerName;
    private String product ;
    private int quantity;
    private int partition;

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public String getCustomerName() {
        return customerName;
    }

    public String getProduct() {
        return product;
    }

    public int getQuantity() {
        return quantity;
    }

    public Order(String customerName, String product, int quantity) {
        this.customerName = customerName;
        this.product = product;
        this.quantity = quantity;
    }
}
