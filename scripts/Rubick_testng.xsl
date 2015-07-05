<!DOCTYPE suite SYSTEM "http://testng.org/testng-1.0.dtd" >

<suite name="rubick_suite" verbose="1">
    <!--listeners>
        <listener class-name="com.flipkart.website.testng.TestListener" />
        <listener class-name="com.flipkart.website.testng.Reporter" />
    </listeners-->

    <test name="Rubick Tests">
        <classes>
            <!--<class name="tests.engage.engage_api.rubick.src.Rubick"/>-->
            <class name="tests.engage.engage_api.rubick.src.Rubick"/>
        </classes>
    </test>
</suite>
