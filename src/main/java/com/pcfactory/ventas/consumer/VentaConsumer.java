package com.pcfactory.ventas.consumer;

import com.pcfactory.ventas.model.Venta;
import com.pcfactory.ventas.repository.VentaRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class VentaConsumer {

    @Autowired
    private VentaRepository ventaRepository;

    @KafkaListener(topics = "${app.kafka.topic.ventas}")
    public void listen(
            @Payload Venta venta,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        // aplicando regla de negocio de comisiones
        Double comision = venta.getValor() * 0.1;
        Double valorConIva = venta.getValor() * 1.19;

        venta.setValor(valorConIva.longValue());

        ventaRepository.save(venta);
    }

}
