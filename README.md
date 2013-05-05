<h2>transactor</h2>
A transactor implementation that manages connection and SQL objects and removes the need for boiler plate code.

<h3>Sample</h3>
<pre>
transactor.queryForInt("Select count(*) from EMPLOYEEE");
</pre>

<h3>dependencies</h3>
<h4>production</h4>
<td><pre>com.google.guava:guava:14.0</pre></td>
<td><pre>org.slf4j:slf4j-log4j12:1.7.4</pre></td>
<td><pre>org.slf4j:slf4j-api:1.7.4</pre></td>
<td><pre>log4j:log4j:1.2.17</pre></td>
<h4>test</h4>
<td><pre>junit:junit:4.8.2</pre></td>
<td><pre>org.hamcrest:hamcrest-all:1.3</pre></td>
<td><pre>org.mockito:mockito-all:1.9.5</pre></td>

