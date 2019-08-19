#This is a two-stage build image for a java project.
#The actual build step happens in the maven container, and the final jar is
#deployed to a more lightweight container. Maven should not download all the
#dependencies if pom.xml is not modified. Modifying only the source code should
#trigger only the compile step

FROM openjdk:8-jre-slim

#Install curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends curl

#This container can access the build artifacts inside the BUILD container.
#Everything that is not copied is discarded
#COPY --from=BUILD /usr/src/app/target/transitdata-rail-tripupdate-source-jar-with-dependencies.jar /usr/app/transitdata-rail-tripupdate-source.jar

ADD target/transitdata-rail-tripupdate-source-jar-with-dependencies.jar /usr/app/transitdata-rail-tripupdate-source.jar

ENTRYPOINT ["java", "-jar", "/usr/app/transitdata-rail-tripupdate-source.jar"]
