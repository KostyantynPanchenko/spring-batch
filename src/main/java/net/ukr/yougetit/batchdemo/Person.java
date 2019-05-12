package net.ukr.yougetit.batchdemo;

public class Person {

    private int age;
    private String firstName;
    private String email;

    public Person() {
    }

    public Person(final int age, final String firstName, final String email) {
        this.age = age;
        this.firstName = firstName;
        this.email = email;
    }

    public int getAge() {
        return age;
    }

    public void setAge(final int age) {
        this.age = age;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(final String firstName) {
        this.firstName = firstName;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(final String email) {
        this.email = email;
    }
}
