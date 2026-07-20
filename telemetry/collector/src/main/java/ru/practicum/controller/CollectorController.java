package ru.practicum.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.practicum.entity.hub.HubEvent;
import ru.practicum.entity.sensor.SensorEvent;
import ru.practicum.kafka.KafkaProducerService;
import ru.practicum.mapper.hub.HubEventMapper;
import ru.practicum.mapper.sensor.SensorEventMapper;

@Slf4j
@RestController
@RequestMapping("/events")
@RequiredArgsConstructor
public class CollectorController {

    private final KafkaProducerService kafkaProducerService;

    @PostMapping("/hubs")
    public void collectHub(@RequestBody HubEvent hubEvent) {
        log.debug("Received hub event: {}", hubEvent);

        try {
            var avroEvent = HubEventMapper.toAvro(hubEvent);
            kafkaProducerService.sendHubEvent(avroEvent);

        } catch (Exception e) {
            log.error("Error processing hub event: {}", hubEvent, e);
            throw e;
        }
    }

    @PostMapping("/sensors")
    public void collectSensor(@RequestBody SensorEvent sensorEvent) {
        log.debug("Received sensor event: {}", sensorEvent);

        try {
            var avroEvent = SensorEventMapper.toAvro(sensorEvent);
            kafkaProducerService.sendSensorEvent(avroEvent);

        } catch (Exception e) {
            log.error("Error processing sensor event: {}", sensorEvent, e);
            throw e;
        }
    }
}