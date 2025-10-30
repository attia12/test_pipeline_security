package fr.tictak.dema.dto.in;

import fr.tictak.dema.model.Item;
import fr.tictak.dema.model.ItemQuantity;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.stream.Collectors;

public record MissionNotification(
        String moveId,
        String sourceAddress,
        String destinationAddress,
        String postCommissionCost,
        String durationInMinutes,
        String numberOfProducts,
        String distanceInKm,
        long remainingSeconds,
        String status,
        String clientName,
        String phoneNumber,
        String plannedDate,  // Can be null
        String plannedTime,
        List<ItemQuantity> items) {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm");

    public LocalDateTime getPlannedDateTime() {
        // If either plannedDate or plannedTime is null, return null instead of throwing an exception
        if (plannedDate == null || plannedTime == null) {
            return null;
        }
        try {
            LocalDate date = LocalDate.parse(plannedDate, DATE_FORMATTER);
            LocalTime time = LocalTime.parse(plannedTime, TIME_FORMATTER);
            return LocalDateTime.of(date, time);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid date or time format. Expected 'dd/MM/yyyy' for date and 'HH:mm' for time.", e);
        }
    }

    public String getItems() {
        if (items == null || items.isEmpty()) {
            return "No items";
        }
        return items.stream()
                .map(item -> item.getItemLabel() + ": " + item.getQuantity())
                .collect(Collectors.joining(", "));
    }
}