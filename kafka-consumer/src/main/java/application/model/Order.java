package application.model;

public class Order {
    private String customerName;
    private String product ;
    private int quantity;

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
}
