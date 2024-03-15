package org.example;

import org.apache.jena.graph.*;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import org.streamreasoning.rsp4j.io.DataStreamImpl;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ISWCStreamGenerator {
    private static final String PREFIX = "http://rsp4j.io/covid/";
    private static final Long TIMEOUT = 1000l;

    private DataStream<Graph> observationStream;
    private DataStream<Graph> covidStream;
    private DataStream<Graph> contactStream;
    private final AtomicBoolean isStreaming;
    private final Random randomGenerator;
    private AtomicLong streamIndexCounter;

    private enum Person {Alice, Bob, Elena, Carl, David, John};
    private enum Room {RedRoom, BlueRoom};
    private enum EventType {RFID, Facebook, ContactTracing, HospitalResult};

    Map<ISWCStreamGenerator.Person, ISWCStreamGenerator.EventType> personsEventTypesMap =
            Map.of(ISWCStreamGenerator.Person.Alice, ISWCStreamGenerator.EventType.RFID,
                    ISWCStreamGenerator.Person.John, ISWCStreamGenerator.EventType.RFID,
                    ISWCStreamGenerator.Person.Bob, ISWCStreamGenerator.EventType.Facebook,
                    ISWCStreamGenerator.Person.Elena, ISWCStreamGenerator.EventType.Facebook);

    Map<ISWCStreamGenerator.Person, ISWCStreamGenerator.Person> isWithMap =
            Map.of(ISWCStreamGenerator.Person.Carl, ISWCStreamGenerator.Person.Bob,
                    ISWCStreamGenerator.Person.David, ISWCStreamGenerator.Person.Elena);

    private ISWCStreamGenerator.Person[] isWithPersons = new ISWCStreamGenerator.Person[]{ISWCStreamGenerator.Person.Carl, ISWCStreamGenerator.Person.David};
    private ISWCStreamGenerator.Person[] evenTyptesPersons = new ISWCStreamGenerator.Person[]{ISWCStreamGenerator.Person.Alice, ISWCStreamGenerator.Person.John, ISWCStreamGenerator.Person.Bob, ISWCStreamGenerator.Person.Elena};

    public ISWCStreamGenerator() {
        this.observationStream = new DataStreamImpl<>(PREFIX+"observations");
        this.covidStream = new DataStreamImpl<>(PREFIX+"testResults");
        this.contactStream = new DataStreamImpl<>(PREFIX+"tracing");
        this.streamIndexCounter = new AtomicLong(0);
        this.isStreaming = new AtomicBoolean(false);
        randomGenerator = new Random(1337);
    }

    public static String getPREFIX() {
        return ISWCStreamGenerator.PREFIX;
    }

    public DataStream<Graph> getObservationStream() {

        return observationStream;
    }
    public DataStream<Graph> getCovidStream(){
        return covidStream;
    }
    public DataStream<Graph> getContactStream(){
        return contactStream;
    }

    public void startStreaming() {
        if (!this.isStreaming.get()) {
            this.isStreaming.set(true);
            Runnable task =
                    () -> {
                        long ts = 0;
                        while (this.isStreaming.get()) {
                            long finalTs = ts;
                            observationStream.put(createRandomObservationEvent(), ts);
                            if (randomGenerator.nextDouble()>0.9){
                                covidStream.put(createRandomCovidEvent(),ts);
                            }
                            if (randomGenerator.nextDouble()>=0.5){
                                contactStream.put(createRandomContactTracingEvent(),ts);
                            }
                            ts += 5*60*1000;
                            try {
                                Thread.sleep(TIMEOUT);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    };

            Thread thread = new Thread(task);
            thread.start();
        }
    }


    private ISWCStreamGenerator.Person selectRandomPerson(ISWCStreamGenerator.Person[] persons){
        int randomIndex = randomGenerator.nextInt((persons.length));
        return persons[randomIndex];
    }
    private ISWCStreamGenerator.Person selectRandomPerson(){
        return selectRandomPerson(ISWCStreamGenerator.Person.values());
    }
    private ISWCStreamGenerator.Room selectRandomRoom(){
        int randomIndex = randomGenerator.nextInt((ISWCStreamGenerator.Room.values().length));
        return ISWCStreamGenerator.Room.values()[randomIndex];
    }
    public Graph createRandomObservationEvent(){
        ISWCStreamGenerator.Person randomPerson = selectRandomPerson(evenTyptesPersons);
        ISWCStreamGenerator.Room randomRoom = selectRandomRoom();
        ISWCStreamGenerator.EventType selectedType = personsEventTypesMap.get(randomPerson);

        Graph graph = Factory.createDefaultGraph();
        Node a = NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        Node person = NodeFactory.createURI("http://rsp4j.io/covid#Person");
        Node room = NodeFactory.createURI("http://rsp4j.io/covid#Room");
        Node door_status = NodeFactory.createURI("http://rsp4j.io/covid#DoorStatus");
        long eventID = streamIndexCounter.incrementAndGet();

        Random rand = new Random();
        int n = rand.nextInt(100);

        switch (selectedType){
            case RFID:
                graph.add(Triple.create(NodeFactory.createURI(PREFIX + "_observation" + eventID), a, NodeFactory.createURI(PREFIX + "RFIDObservation")));
                graph.add(Triple.create(NodeFactory.createURI(PREFIX + "_observation" + eventID), NodeFactory.createURI(PREFIX + "where"), NodeFactory.createURI(PREFIX + randomRoom)));
                graph.add(Triple.create(NodeFactory.createURI(PREFIX + "_observation" + eventID), NodeFactory.createURI(PREFIX + "who"), NodeFactory.createURI(PREFIX + randomPerson)));
                graph.add(Triple.create(NodeFactory.createURI(PREFIX + randomPerson), NodeFactory.createURI(PREFIX + "isIn"), NodeFactory.createURI(PREFIX + randomRoom)));
                graph.add(Triple.create(NodeFactory.createURI(PREFIX + randomPerson), a, person));
                graph.add(Triple.create(NodeFactory.createURI(PREFIX + randomRoom), a, room));
                if(n > 80){
                    graph.add(Triple.create(NodeFactory.createURI(PREFIX + randomRoom), NodeFactory.createURI(PREFIX + "hasDoorStatus"), NodeFactory.createURI(PREFIX + "openStatus")));
                    graph.add(Triple.create(NodeFactory.createURI(PREFIX + "openStatus"), a, door_status));
                }else if(n < 20){
                    graph.add(Triple.create(NodeFactory.createURI(PREFIX + randomRoom), NodeFactory.createURI(PREFIX + "hasDoorStatus"), NodeFactory.createURI(PREFIX + "closedStatus")));
                    graph.add(Triple.create(NodeFactory.createURI(PREFIX + "closedStatus"), a, door_status));
                }

                break;
            case Facebook:
                graph.add(Triple.create(NodeFactory.createURI(PREFIX + "_observation" + eventID), a, NodeFactory.createURI(PREFIX + "FacebookPost")));
                graph.add(Triple.create(NodeFactory.createURI(PREFIX + "_observation" + eventID), NodeFactory.createURI(PREFIX + "where"), NodeFactory.createURI(PREFIX + randomRoom)));
                graph.add(Triple.create(NodeFactory.createURI(PREFIX + "_observation" + eventID), NodeFactory.createURI(PREFIX + "who"), NodeFactory.createURI(PREFIX + randomPerson)));
                graph.add(Triple.create(NodeFactory.createURI(PREFIX + randomPerson), NodeFactory.createURI(PREFIX + "isIn"), NodeFactory.createURI(PREFIX + randomRoom)));
                graph.add(Triple.create(NodeFactory.createURI(PREFIX + randomPerson), a, person));
                graph.add(Triple.create(NodeFactory.createURI(PREFIX + randomRoom), a, room));

                if(n > 80){
                    graph.add(Triple.create(NodeFactory.createURI(PREFIX + randomRoom), NodeFactory.createURI(PREFIX + "hasDoorStatus"), NodeFactory.createURI(PREFIX + "openStatus")));
                    graph.add(Triple.create(NodeFactory.createURI(PREFIX + "openStatus"), a, door_status));
                }else if(n < 20){
                    graph.add(Triple.create(NodeFactory.createURI(PREFIX + randomRoom), NodeFactory.createURI(PREFIX + "hasDoorStatus"), NodeFactory.createURI(PREFIX + "closedStatus")));
                    graph.add(Triple.create(NodeFactory.createURI(PREFIX + "closedStatus"), a, door_status));
                }
                break;
            default:
                break;
        }
        return graph;
    }
    public Graph createRandomCovidEvent(){
        ISWCStreamGenerator.Person randomPerson = selectRandomPerson();


        Graph graph = Factory.createDefaultGraph();
        Node a = NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        long eventID = streamIndexCounter.incrementAndGet();

        graph.add(Triple.create(NodeFactory.createURI(PREFIX + "_observation" + eventID), a, NodeFactory.createURI(PREFIX + "TestResultPost")));
        graph.add(Triple.create(NodeFactory.createURI(PREFIX + "_observation" + eventID), NodeFactory.createURI(PREFIX + "who"), NodeFactory.createURI(PREFIX + randomPerson)));
        graph.add(Triple.create(NodeFactory.createURI(PREFIX + "_observation" + eventID), NodeFactory.createURI(PREFIX + "hasResult"), NodeFactory.createURI(PREFIX + "positive")));
        return graph;
    }

    public Graph createRandomContactTracingEvent(){
        ISWCStreamGenerator.Person randomPerson = selectRandomPerson(isWithPersons);

        Graph graph = Factory.createDefaultGraph();
        Node a = NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
        long eventID = streamIndexCounter.incrementAndGet();

        graph.add(Triple.create(NodeFactory.createURI(PREFIX + "_observation" + eventID), a, NodeFactory.createURI(PREFIX + "ContactTracingPost")));
        graph.add(Triple.create(NodeFactory.createURI(PREFIX + "_observation" + eventID), NodeFactory.createURI(PREFIX + "who"), NodeFactory.createURI(PREFIX + randomPerson)));
        graph.add(Triple.create(NodeFactory.createURI(PREFIX + randomPerson), NodeFactory.createURI(PREFIX + "isWith"), NodeFactory.createURI(PREFIX + isWithMap.get(randomPerson))));

        return graph;
    }



    public void stopStreaming() {
        this.isStreaming.set(false);
    }

    public static void main(String[] args){
        ISWCStreamGenerator gen = new ISWCStreamGenerator();
        for (int i = 0; i < 1000; i++) {
            System.out.println("New event:");
            System.out.println(gen.createRandomObservationEvent());
        }
    }
}
